# frozen_string_literal: true

require_relative "lib/durable_flow/version"

Gem::Specification.new do |spec|
  spec.name = "durable_flow"
  spec.version = DurableFlow::VERSION
  spec.authors = [ "DurableFlow contributors" ]
  spec.email = [ "dev@example.com" ]

  spec.summary = "Durable workflows on Rails Active Job continuations."
  spec.description = "An Inngest-style durable workflow runtime built on ActiveJob::Continuable, Active Record, and Rails.event."
  spec.homepage = "https://github.com/skorfmann/durableflow"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir[
    "app/**/*",
    "config/**/*",
    "lib/**/*",
    "MIT-LICENSE",
    "README.md"
  ].select { |path| File.file?(path) }

  spec.require_paths = [ "lib" ]

  spec.add_dependency "actionpack", ">= 8.1.0", "< 9.0"
  spec.add_dependency "activejob", ">= 8.1.0", "< 9.0"
  spec.add_dependency "activerecord", ">= 8.1.0", "< 9.0"
  spec.add_dependency "activesupport", ">= 8.1.0", "< 9.0"
  spec.add_dependency "railties", ">= 8.1.0", "< 9.0"

  spec.add_development_dependency "minitest", "~> 5.20"
  spec.add_development_dependency "solid_queue", "1.1.2"
  spec.add_development_dependency "sqlite3", "~> 2.0"
end
