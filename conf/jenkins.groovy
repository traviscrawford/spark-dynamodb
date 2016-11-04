job('spark-dynamodb') {
    logRotator {
        numToKeep(3)
    }
    scm {
        git {
            remote {
                github('Levelmoney/spark-dynamodb', 'ssh')
                branch('develop')
                credentials('github.com')
            }
        }
    }
    triggers {
        scm('H/15 * * * *')
    }
    steps {
        environmentVariables {
            env('AWS_ACCESS_KEY_ID', 'AKIAITXOIIW6IAQSUHNQ')
            env('AWS_SECRET_ACCESS_KEY', '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')
        }
        sbt('SBT0.13', '+publish')
    }
}