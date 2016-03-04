Embulk::JavaPlugin.register_input(
  "dynamodb", "org.embulk.input.dynamodb.DynamodbInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
