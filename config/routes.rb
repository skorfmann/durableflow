# frozen_string_literal: true

DurableFlow::Engine.routes.draw do
  root to: "workflow_runs#index"
  resources :workflow_runs, only: [ :index, :show ], param: :run_id
end
