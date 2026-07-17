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

    UniFFILib.attach_function :prolly_fast_proximity_search,
                              [:uint64, :pointer, :size_t, :uint32, :uint64],
                              FastPageResult.by_value
    UniFFILib.attach_function :prolly_fast_page_release, [:uint64], :void

    class Scope
      def initialize = @alive = true
      def close = @alive = false

      def check!
        raise 'packed page view escaped its callback scope' unless @alive
      end
    end

    FieldView = Struct.new(:pointer, :offset, :length, :scope) do
      def bytes
        scope.check!
        pointer.get_bytes(offset, length).b
      end
    end

    NeighborView = Struct.new(:key, :distance, :rank, :value, :proof, keyword_init: true)

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
  end
end
