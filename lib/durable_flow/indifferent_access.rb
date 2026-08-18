# frozen_string_literal: true

module DurableFlow
  module IndifferentAccess
    module_function

    def fetch(collection, key)
      return unless collection.respond_to?(:[])
      return collection[key] unless collection.respond_to?(:key?)
      return collection[key] if collection.key?(key)
      return collection[key.to_s] if collection.key?(key.to_s)
      return collection[key.to_sym] if key.respond_to?(:to_sym) && collection.key?(key.to_sym)

      collection[key]
    end
  end
end
