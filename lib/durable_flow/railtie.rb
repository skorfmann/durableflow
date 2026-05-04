# frozen_string_literal: true

module DurableFlow
  class Railtie < ::Rails::Railtie
    initializer "durable_flow.subscribe_to_events", after: :load_config_initializers do
      ActiveSupport.on_load(:active_record) do
        DurableFlow.subscribe_to_rails_events!
      end
    end
  end
end
