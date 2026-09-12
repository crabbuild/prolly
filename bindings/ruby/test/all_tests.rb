# frozen_string_literal: true

require 'rbconfig'

# Run the procedural wire/engine smoke contract and the Minitest portable API
# contract (including TurboQuant) in separate processes. This also guarantees
# that UniFFI finalizers from one suite finish before the next suite starts.
library = File.expand_path('../lib', __dir__)
%w[prolly_smoke_test.rb portable_parity_test.rb].each do |test|
  command = [RbConfig.ruby, "-I#{library}", File.expand_path(test, __dir__)]
  abort "Ruby binding test failed: #{test}" unless system(*command)
end
