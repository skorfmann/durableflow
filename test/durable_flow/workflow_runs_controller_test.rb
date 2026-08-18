# frozen_string_literal: true

require "test_helper"
require "action_controller"

load File.expand_path("../../config/routes.rb", __dir__)
require_relative "../../app/controllers/durable_flow/workflow_runs_controller"

DurableFlow::WorkflowRunsController.prepend_view_path(
  File.expand_path("../../app/views", __dir__),
)
DurableFlow::WorkflowRunsController.include(DurableFlow::Engine.routes.url_helpers)
DurableFlow::WorkflowRunsController.helper(DurableFlow::Engine.routes.url_helpers)

class DurableFlowWorkflowRunsControllerTest < ActionController::TestCase
  tests DurableFlow::WorkflowRunsController

  setup do
    @routes = DurableFlow::Engine.routes
    @original_basic_auth = DurableFlow.ui_http_basic_auth
    @original_allow = DurableFlow.ui_allow_unauthenticated_access
    DurableFlow::WorkflowRun.delete_all
  end

  teardown do
    DurableFlow.ui_http_basic_auth = @original_basic_auth
    DurableFlow.ui_allow_unauthenticated_access = @original_allow
  end

  test "denies access by default" do
    get :index

    assert_response :forbidden
    assert_match(/denied by default/, response.body)
  end

  test "requires valid credentials when basic auth is configured" do
    DurableFlow.ui_http_basic_auth = { name: "ops", password: "s3cret" }

    get :index
    assert_response :unauthorized

    request.env["HTTP_AUTHORIZATION"] =
      ActionController::HttpAuthentication::Basic.encode_credentials("ops", "wrong")
    get :index
    assert_response :unauthorized

    request.env["HTTP_AUTHORIZATION"] =
      ActionController::HttpAuthentication::Basic.encode_credentials("ops", "s3cret")
    get :index
    assert_response :success
  end

  test "allows access when unauthenticated access is explicitly enabled" do
    DurableFlow.ui_allow_unauthenticated_access = true

    get :index

    assert_response :success
  end
end
