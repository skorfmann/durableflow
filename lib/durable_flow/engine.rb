# frozen_string_literal: true

module DurableFlow
  class Engine < ::Rails::Engine
    isolate_namespace DurableFlow
  end
end
