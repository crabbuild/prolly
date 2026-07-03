# frozen_string_literal: true

require 'prolly'

engine = Prolly::ProllyEngine.memory(Prolly.default_config)
tree = engine.create
tree = engine.put(tree, 'user:001'.b, 'Ada'.b)
tree = engine.put(tree, 'user:002'.b, 'Grace'.b)
tree = engine.put(tree, 'user:003'.b, 'Linus'.b)

raise 'expected user:001' unless engine.get(tree, 'user:001'.b) == 'Ada'.b

tree = engine.delete(tree, 'user:003'.b)
raise 'expected deleted user:003' unless engine.get(tree, 'user:003'.b).nil?

users = engine.range(tree, 'user:'.b, 'user;'.b)
expected = [['user:001'.b, 'Ada'.b], ['user:002'.b, 'Grace'.b]]
actual = users.map { |entry| [entry.key, entry.value] }
raise 'unexpected users' unless actual == expected

puts "basic_map: #{users.length} users in range"
