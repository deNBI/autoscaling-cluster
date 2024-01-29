params.memoryLimit = "60 GB"
params.numberOfTasks = 10

process sleep {

  memory params.memoryLimit

  input:
    val x

  """
  sleep 10
  """
}

workflow {
  channel.from(0..params.numberOfTasks) | sleep
}
