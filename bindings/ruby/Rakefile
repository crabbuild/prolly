# frozen_string_literal: true

require 'rake/testtask'

Rake::TestTask.new(:test) do |task|
  task.libs << 'lib'
  task.pattern = 'test/**/*_test.rb'
end

task default: :test
