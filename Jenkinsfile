#!groovy
def utils = new camp.ssc.builds.Utils()
def builtImg = utils.getRepoName()
def buildRev = utils.getBuildRev()
def builderName = "${env.BRANCH_NAME}-build-${builtImg}-${buildRev}"
def builderImg = "${builderName}-image"

node('r_n_d') {
    currentBuild.displayName = buildRev

    checkout scm

    glideInstall()

    tryBuildRD({stage('building app') {
        def builderImage = docker.build(builderImg, '-f Dockerfile.build .')
        if (sh(returnStatus:true, script: "docker inspect ${builderName}") == 0) {
            sh "docker rm ${builderName}"
        }
        sh "docker run --name ${builderName} ${builderImg}"
        sh "docker cp ${builderName}:/etc/ssl/certs/ca-certificates.crt ."
        sh "docker cp ${builderName}:/out/${builtImg} ."
        sh "docker rm ${builderName}"
        sh "docker rmi ${builderImg}"
    }})

    tryDeployRD({stage('publishing container') {
        docker.withRegistry('https://registry.daymax.xyz/') {
            def builtImage = docker.build(builtImg, '-f Dockerfile .')
            if (env.BRANCH_NAME == 'master') {
                builtImage.push 'latest'
                builtImage.push "${buildRev}"
            } else {
                builtImage.push "${buildRev}-${env.BRANCH_NAME}"
            }
        }
    }}, 'docker registry')

}
