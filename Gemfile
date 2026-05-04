# frozen_string_literal: true

source "https://rubygems.org"

ruby ">= 3.2.0"

gemspec

rails_path = File.expand_path("vendor/rails", __dir__)

if Dir.exist?(rails_path)
  gem "actionpack", path: File.join(rails_path, "actionpack")
  gem "actionview", path: File.join(rails_path, "actionview")
  gem "activejob", path: File.join(rails_path, "activejob")
  gem "activemodel", path: File.join(rails_path, "activemodel")
  gem "activerecord", path: File.join(rails_path, "activerecord")
  gem "activesupport", path: File.join(rails_path, "activesupport")
  gem "railties", path: File.join(rails_path, "railties")
else
  rails_options = {
    git: "https://github.com/rails/rails.git",
    ref: "5b4cf30c08a2dd022ed8c56b2958aff7c81ca4b6",
  }

  gem "actionpack", **rails_options, glob: "actionpack/*.gemspec"
  gem "actionview", **rails_options, glob: "actionview/*.gemspec"
  gem "activejob", **rails_options, glob: "activejob/*.gemspec"
  gem "activemodel", **rails_options, glob: "activemodel/*.gemspec"
  gem "activerecord", **rails_options, glob: "activerecord/*.gemspec"
  gem "activesupport", **rails_options, glob: "activesupport/*.gemspec"
  gem "railties", **rails_options, glob: "railties/*.gemspec"
end

gem "solid_queue", "1.1.2"
gem "sqlite3", "~> 2.0"
