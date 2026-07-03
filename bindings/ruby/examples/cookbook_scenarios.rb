# frozen_string_literal: true

require 'rbconfig'

SCENARIOS = [
  'batch_build.rb',
  'local_first_state.rb',
  'resolver.rb',
  'crdt_merge.rb',
  'conversation_memory.rb',
  'agent_event_log.rb',
  'background_compaction.rb',
  'deterministic_rag_snapshot.rb',
  'document_chunk_index.rb',
  'vector_sidecar.rb',
  'provenance_values.rb',
  'materialized_view.rb',
  'filesystem_snapshot.rb',
  'durable_sqlite.rb',
].freeze

here = __dir__
SCENARIOS.each do |scenario|
  ok = system(RbConfig.ruby, File.join(here, scenario))
  exit($?.exitstatus || 1) unless ok
end
