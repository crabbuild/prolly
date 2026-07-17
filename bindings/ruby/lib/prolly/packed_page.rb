# frozen_string_literal: true

module Prolly
  module PackedPage
    class FastPageResult < FFI::Struct
      layout :status, :int32,
             :terminal, :uint8,
             :reserved, [:uint8, 3],
             :record_count, :uint32,
             :lease_handle, :uint64,
             :data_ptr, :pointer,
             :data_len, :uint64
    end

    class FastScanOpenResult < FFI::Struct
      layout :status, :int32,
             :reserved, :uint32,
             :scan_handle, :uint64
    end


    class FastValueLeaseResult < FFI::Struct
      layout :status, :int32,
             :found, :uint8,
             :reserved, [:uint8, 3],
             :lease_handle, :uint64,
             :data_ptr, :pointer,
             :data_len, :uint64
    end

    UniFFILib.attach_function :prolly_fast_proximity_search,
                              [:uint64, :pointer, :size_t, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_proximity_scan_page,
                              [:uint64, :pointer, :size_t, :uint8, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_proximity_scan_range_page,
                              [:uint64, :pointer, :size_t, :pointer, :size_t, :uint8,
                               :pointer, :size_t, :uint8, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_page_release, [:uint64], :void
    UniFFILib.attach_function :prolly_fast_read_session_scan_open,
                              [:uint64, :pointer, :size_t, :pointer, :size_t, :uint8],
                              FastScanOpenResult.by_value
    UniFFILib.attach_function :prolly_fast_read_session_scan_next,
                              [:uint64, :uint64, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_scan_close, [:uint64], :void
    UniFFILib.attach_function :prolly_fast_read_session_get_lease,
                              [:uint64, :pointer, :size_t],
                              FastValueLeaseResult.by_value
    UniFFILib.attach_function :prolly_fast_proximity_get_lease,
                              [:uint64, :pointer, :size_t],
                              FastValueLeaseResult.by_value
    UniFFILib.attach_function :prolly_fast_indexed_get_lease,
                              [:uint64, :pointer, :size_t],
                              FastValueLeaseResult.by_value
    UniFFILib.attach_function :prolly_fast_value_release, [:uint64], :void

    class Scope
      def initialize = @alive = true
      def close = @alive = false

      def check!
        raise 'packed page view escaped its callback scope' unless @alive
      end
    end

    class FieldView
      attr_reader :length

      def initialize(pointer, offset, length, scope)
        @pointer = pointer
        @offset = offset
        @length = length
        @scope = scope
      end

      def bytes
        @scope.check!
        @pointer.get_bytes(@offset, @length).b
      end

      alias to_s bytes

      def subview(start, length = @length - start)
        @scope.check!
        raise RangeError, 'scoped subview is out of bounds' if start.negative? || length.negative? ||
                                                             start > @length || length > @length - start
        FieldView.new(@pointer, @offset + start, length, @scope)
      end

      def compare(other)
        @scope.check!
        other.__send__(:check_scope!)
        [@length, other.length].min.times do |index|
          comparison = byte_at(index) <=> other.__send__(:byte_at, index)
          return comparison unless comparison.zero?
        end
        @length <=> other.length
      end

      def compare_bytes_left(left)
        @scope.check!
        [left.bytesize, @length].min.times do |index|
          comparison = left.getbyte(index) <=> byte_at(index)
          return comparison unless comparison.zero?
        end
        left.bytesize <=> @length
      end

      private

      def check_scope! = @scope.check!
      def byte_at(index) = @pointer.get_uint8(@offset + index)
    end

    NeighborView = Struct.new(:key, :distance, :rank, :value, :proof, keyword_init: true)
    ProximityRecordView = Struct.new(:key, :vector, :value, keyword_init: true)
    EntryView = Struct.new(:key, :value, keyword_init: true)
    ScanOutcome = Struct.new(:visited, :stopped, keyword_init: true)
    ValueRefView = Struct.new(:kind, :inline, :cid, :length, keyword_init: true)

    class VectorView
      attr_reader :length

      def initialize(pointer, offset, byte_length, scope)
        raise 'packed proximity vector length is invalid' unless (byte_length % 4).zero?
        @pointer = pointer
        @offset = offset
        @length = byte_length / 4
        @scope = scope
      end

      def component(index)
        @scope.check!
        raise IndexError, 'proximity vector index is out of range' unless index.between?(0, @length - 1)
        @pointer.get_bytes(@offset + index * 4, 4).unpack1('e')
      end

      alias [] component

      def to_a
        @scope.check!
        @pointer.get_bytes(@offset, @length * 4).unpack('e*')
      end
    end

    module_function

    def point_read_view(session_handle, key)
      raise ArgumentError, 'point-read visitor block is required' unless block_given?

      key = key.b
      result = UniFFILib.prolly_fast_read_session_get_lease(
        session_handle, memory_for(key), key.bytesize
      )
      raise "native retained point read failed with status #{result[:status]}" unless result[:status].zero?
      unless result[:found].zero?
        lease = result[:lease_handle]
        raise 'native point read returned an invalid value lease' if lease.zero? ||
                                                                  (result[:data_len].positive? && result[:data_ptr].null?)
        scope = Scope.new
        begin
          return [true, yield(FieldView.new(result[:data_ptr], 0, result[:data_len], scope))]
        ensure
          scope.close
          UniFFILib.prolly_fast_value_release(lease)
        end
      end
      unless result[:lease_handle].zero?
        UniFFILib.prolly_fast_value_release(result[:lease_handle])
        raise 'missing point read returned a value lease'
      end
      [false, nil]
    end

    def proximity_point_read_view(map_handle, key)
      raise ArgumentError, 'proximity record visitor block is required' unless block_given?

      key = key.b
      result = UniFFILib.prolly_fast_proximity_get_lease(map_handle, memory_for(key), key.bytesize)
      raise "native retained proximity read failed with status #{result[:status]}" unless result[:status].zero?
      if result[:found].zero?
        raise 'missing proximity read returned a value lease' unless result[:lease_handle].zero?
        return [false, nil]
      end
      lease = result[:lease_handle]
      pointer = result[:data_ptr]
      length = result[:data_len]
      raise 'native proximity read returned an invalid value lease' if lease.zero? || pointer.null? || length < 8
      scope = Scope.new
      begin
        raise 'invalid retained proximity record header' unless pointer.get_bytes(0, 6) == "PRVR\x02\x01".b
        dimensions, vector_start = read_varint(pointer, 6, length)
        vector_bytes = dimensions * 4
        raise 'retained proximity vector is truncated' if vector_start + vector_bytes > length
        value_length, value_start = read_varint(pointer, vector_start + vector_bytes, length)
        raise 'retained proximity value length is invalid' unless value_start + value_length == length
        view = ProximityRecordView.new(
          vector: VectorView.new(pointer, vector_start, vector_bytes, scope),
          value: FieldView.new(pointer, value_start, value_length, scope)
        )
        [true, yield(view)]
      ensure
        scope.close
        UniFFILib.prolly_fast_value_release(lease)
      end
    end

    def indexed_point_read_view(map_handle, key)
      raise ArgumentError, 'indexed point-read visitor block is required' unless block_given?

      key = key.b
      result = UniFFILib.prolly_fast_indexed_get_lease(map_handle, memory_for(key), key.bytesize)
      raise "native indexed point read failed with status #{result[:status]}" unless result[:status].zero?
      if result[:found].zero?
        raise 'missing indexed point read returned a value lease' unless result[:lease_handle].zero?
        return [false, nil]
      end
      lease = result[:lease_handle]
      raise 'native indexed point read returned an invalid value lease' if lease.zero? ||
                                                                  (result[:data_len].positive? && result[:data_ptr].null?)
      scope = Scope.new
      begin
        [true, yield(FieldView.new(result[:data_ptr], 0, result[:data_len], scope))]
      ensure
        scope.close
        UniFFILib.prolly_fast_value_release(lease)
      end
    end

    def read_varint(pointer, offset, length)
      value = 0
      shift = 0
      while offset < length && shift < 64
        byte = pointer.get_uint8(offset)
        offset += 1
        value |= (byte & 0x7f) << shift
        return [value, offset] if (byte & 0x80).zero?
        shift += 7
      end
      raise 'invalid proximity record varint'
    end

    def value_ref_view(session_handle, key)
      raise ArgumentError, 'value reference visitor block is required' unless block_given?

      point_read_view(session_handle, key) do |value|
        yield decode_value_ref_view(value)
      end
    end

    def decode_value_ref_view(value)
      return ValueRefView.new(kind: :inline, inline: value) if value.length < 4 || value.subview(0, 4).bytes != 'PLVB'.b
      raise 'invalid or unsupported value reference header' if value.length < 6 || value.subview(4, 1).bytes.getbyte(0) != 1

      case value.subview(5, 1).bytes.getbyte(0)
      when 0
        raise 'inline value reference is truncated' if value.length < 14
        length = value.subview(6, 8).bytes.unpack1('Q>')
        raise 'inline value reference length does not match payload' unless value.length == 14 + length
        ValueRefView.new(kind: :inline, inline: value.subview(14, length))
      when 1
        raise 'blob value reference length is invalid' unless value.length == 46
        ValueRefView.new(
          kind: :blob, cid: value.subview(6, 32).bytes,
          length: value.subview(38, 8).bytes.unpack1('Q>')
        )
      else
        raise 'unknown value reference tag'
      end
    end

    def proximity_search_view(map_handle, query, k, max_arena_bytes: 64 * 1024 * 1024)
      query_bytes = query.map(&:to_f).pack('e*')
      query_pointer = FFI::MemoryPointer.new(:char, query_bytes.bytesize)
      query_pointer.put_bytes(0, query_bytes)
      result = UniFFILib.prolly_fast_proximity_search(
        map_handle, query_pointer, query.length, k, max_arena_bytes
      )
      raise "native proximity search failed with status #{result[:status]}" unless result[:status].zero?

      begin
        pointer = result[:data_ptr]
        scope = Scope.new
        header = pointer.get_bytes(0, 28).unpack('a4vvVVVQ<')
        magic, version, kind, _flags, count, table_bytes, arena_bytes = header
        raise 'invalid proximity packed page' unless magic == 'PRPG' && version == 2 && kind == 7
        raise 'invalid proximity packed table' unless table_bytes == count * 40
        arena_start = 28 + table_bytes
        raise 'invalid proximity packed length' unless arena_start + arena_bytes == result[:data_len]

        rows = count.times.map do |index|
          base = 28 + index * 40
          flags, key_offset, key_length = pointer.get_bytes(base, 12).unpack('V3')
          distance = pointer.get_bytes(base + 12, 8).unpack1('E')
          rank, value_offset, value_length, proof_offset, proof_length =
            pointer.get_bytes(base + 20, 20).unpack('V5')
          NeighborView.new(
            key: FieldView.new(pointer, arena_start + key_offset, key_length, scope),
            distance: distance,
            rank: rank,
            value: flags.anybits?(1) ? FieldView.new(pointer, arena_start + value_offset, value_length, scope) : nil,
            proof: flags.anybits?(2) ? FieldView.new(pointer, arena_start + proof_offset, proof_length, scope) : nil
          )
        end
        yield rows
      ensure
        scope&.close
        UniFFILib.prolly_fast_page_release(result[:lease_handle])
      end
    end

    def proximity_scan_view(
      map_handle, start: ''.b, range_end: nil,
      max_records: 4096, max_arena_bytes: 4 * 1024 * 1024
    )
      raise ArgumentError, 'proximity scan visitor block is required' unless block_given?
      raise ArgumentError, 'packed scan limits must be positive' unless max_records.positive? && max_arena_bytes.positive?

      visited = 0
      after = nil
      start = start.b
      range_end = range_end&.b
      loop do
        after_pointer = after.nil? ? nil : memory_for(after)
        result = UniFFILib.prolly_fast_proximity_scan_range_page(
          map_handle,
          memory_for(start), start.bytesize,
          range_end.nil? ? nil : memory_for(range_end), range_end&.bytesize || 0, range_end.nil? ? 0 : 1,
          after_pointer, after&.bytesize || 0, after.nil? ? 0 : 1,
          max_records, max_arena_bytes
        )
        raise "native proximity scan failed with status #{result[:status]}" unless result[:status].zero?

        scope = Scope.new
        begin
          pointer = result[:data_ptr]
          raise 'native proximity scan page pointer was null' if pointer.null?
          magic, version, kind, flags, count, table_bytes, arena_bytes =
            pointer.get_bytes(0, 28).unpack('a4vvVVVQ<')
          valid = magic == 'PRPG' && version == 2 && kind == 8 &&
                  count == result[:record_count] && table_bytes == count * 24 &&
                  flags.anybits?(1) == !result[:terminal].zero? &&
                  28 + table_bytes + arena_bytes == result[:data_len]
          raise 'invalid packed proximity-record page' unless valid

          arena_start = 28 + table_bytes
          previous = nil
          stopped = false
          count.times do |index|
            key_offset, key_length, vector_offset, vector_length, value_offset, value_length =
              pointer.get_bytes(28 + index * 24, 24).unpack('V6')
            require_range!(key_offset, key_length, arena_bytes, 'proximity key')
            require_range!(vector_offset, vector_length, arena_bytes, 'proximity vector')
            require_range!(value_offset, value_length, arena_bytes, 'proximity value')
            key = FieldView.new(pointer, arena_start + key_offset, key_length, scope)
            ordered = if previous
                        previous.compare(key).negative?
                      elsif after
                        key.compare_bytes_left(after).negative?
                      else
                        true
                      end
            raise 'packed proximity keys are not strictly ordered' unless ordered
            previous = key
            visited += 1
            row = ProximityRecordView.new(
              key: key,
              vector: VectorView.new(pointer, arena_start + vector_offset, vector_length, scope),
              value: FieldView.new(pointer, arena_start + value_offset, value_length, scope)
            )
            unless yield row
              stopped = true
              break
            end
          end
          return ScanOutcome.new(visited: visited, stopped: true) if stopped
          after = previous&.bytes
        ensure
          scope.close
          UniFFILib.prolly_fast_page_release(result[:lease_handle])
        end
        return ScanOutcome.new(visited: visited, stopped: false) unless result[:terminal].zero?
        raise 'non-terminal proximity scan page made no progress' unless after
      end
    end

    def scan_range_view(
      session_handle, start, range_end = nil, max_records: 4096,
      max_arena_bytes: 4 * 1024 * 1024
    )
      raise ArgumentError, 'scan visitor block is required' unless block_given?
      raise ArgumentError, 'packed scan limits must be positive' unless max_records.positive? && max_arena_bytes.positive?

      start = start.b
      range_end = range_end&.b
      start_pointer = memory_for(start)
      end_pointer = range_end.nil? ? nil : memory_for(range_end)
      opened = UniFFILib.prolly_fast_read_session_scan_open(
        session_handle, start_pointer, start.bytesize, end_pointer,
        range_end&.bytesize || 0, range_end.nil? ? 0 : 1
      )
      raise "native retained scan open failed with status #{opened[:status]}" unless opened[:status].zero?

      visited = 0
      previous_page_key = nil
      begin
        loop do
          result = UniFFILib.prolly_fast_read_session_scan_next(
            session_handle, opened[:scan_handle], max_records, max_arena_bytes
          )
          raise "native retained scan read failed with status #{result[:status]}" unless result[:status].zero?

          scope = Scope.new
          begin
            pointer = result[:data_ptr]
            raise 'native packed scan page pointer was null' if pointer.null?
            magic, version, kind, flags, count, table_bytes, arena_bytes =
              pointer.get_bytes(0, 28).unpack('a4vvVVVQ<')
            valid = magic == 'PRPG' && version == 1 && kind == 1 &&
                    count == result[:record_count] && table_bytes >= count * 16 &&
                    (table_bytes % 16).zero? && flags.anybits?(1) == !result[:terminal].zero? &&
                    28 + table_bytes + arena_bytes == result[:data_len]
            raise 'invalid retained packed scan page' unless valid

            arena_start = 28 + table_bytes
            previous_view = nil
            stopped = false
            count.times do |index|
              key_offset, key_length, value_offset, value_length =
                pointer.get_bytes(28 + index * 16, 16).unpack('V4')
              require_range!(key_offset, key_length, arena_bytes, 'scan key')
              require_range!(value_offset, value_length, arena_bytes, 'scan value')
              key = FieldView.new(pointer, arena_start + key_offset, key_length, scope)
              value = FieldView.new(pointer, arena_start + value_offset, value_length, scope)
              ordered = if previous_view
                          previous_view.compare(key).negative?
                        elsif previous_page_key
                          key.compare_bytes_left(previous_page_key).negative?
                        else
                          true
                        end
              raise 'packed scan page keys are not strictly ordered' unless ordered
              previous_view = key
              visited += 1
              unless yield EntryView.new(key: key, value: value)
                stopped = true
                break
              end
            end
            return ScanOutcome.new(visited: visited, stopped: true) if stopped
            previous_page_key = previous_view&.bytes
          ensure
            scope.close
            UniFFILib.prolly_fast_page_release(result[:lease_handle])
          end
          return ScanOutcome.new(visited: visited, stopped: false) unless result[:terminal].zero?
          raise 'non-terminal packed scan page made no progress' unless previous_page_key
        end
      ensure
        UniFFILib.prolly_fast_scan_close(opened[:scan_handle])
      end
    end

    def memory_for(bytes)
      return nil if bytes.empty?

      FFI::MemoryPointer.new(:uint8, bytes.bytesize).tap { |pointer| pointer.put_bytes(0, bytes) }
    end
    private_class_method :memory_for

    def require_range!(offset, length, arena_bytes, field)
      raise "#{field} is outside the packed page arena" if offset > arena_bytes || length > arena_bytes - offset
    end
    private_class_method :require_range!

  end
end
