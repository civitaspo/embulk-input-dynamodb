Embulk::JavaPlugin.register_input(
  "dynamodb", "org.embulk.input.DynamodbInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
