# frozen_string_literal: true

module DurableFlow
  class ApplicationRecord < ActiveRecord::Base
    self.abstract_class = true
  end
end
