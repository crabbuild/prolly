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

    UniFFILib.attach_function :prolly_fast_proximity_search,
                              [:uint64, :pointer, :size_t, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_page_release, [:uint64], :void
    UniFFILib.attach_function :prolly_fast_read_session_scan_open,
                              [:uint64, :pointer, :size_t, :pointer, :size_t, :uint8],
                              FastScanOpenResult.by_value
    UniFFILib.attach_function :prolly_fast_read_session_scan_next,
                              [:uint64, :uint64, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_scan_close, [:uint64], :void

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
    EntryView = Struct.new(:key, :value, keyword_init: true)
    ScanOutcome = Struct.new(:visited, :stopped, keyword_init: true)

    module_function

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
