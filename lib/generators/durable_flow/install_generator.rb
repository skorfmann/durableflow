# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module DurableFlow
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      def copy_migration
        migration_template "create_durable_flow_tables.rb", "db/migrate/create_durable_flow_tables.rb"
      end

      def self.next_migration_number(dirname)
        ActiveRecord::Generators::Base.next_migration_number(dirname)
      end
    end
  end
end
