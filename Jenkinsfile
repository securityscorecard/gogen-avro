#!groovy

import camp.ssc.builds.RDVars
import camp.ssc.builds.Utils

node {
    stageGoInitWorkspace()
    parallel ([
        Testing: stageClosure('Testing', {
            def rd = new RDVars()
            def utils = new Utils()
            def repo = utils.getRepoName()
            docker.image(rd.goJenkinsImg()).inside("-v ${pwd()}:/go/src/github.com/securityscorecard/${repo}") {
                sh """
                cd \$GOPATH/src/github.com/securityscorecard/${repo} && \
                ./test.sh
                """
            }
        }),
        Vetting: closureGoVet()
    ])
    stageDockerize('https://registry.daymax.xyz/')
}
