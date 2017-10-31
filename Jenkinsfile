#!groovy
node {
    stageGoInitWorkspace()
    parallel ([
        Vetting: closureGoVet()
    ])
    stageDockerize('https://registry.daymax.xyz/')
}
