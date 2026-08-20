// Alternative CI path to .github/workflows/ci.yml — same gates.
pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 30, unit: 'MINUTES')
    }

    environment {
        MARSHAL_TEST_DSN = 'postgres://marshal:marshal@localhost:5433/marshal?sslmode=disable'
    }

    stages {
        stage('postgres') {
            steps {
                sh '''
                    docker rm -f marshal-ci-pg || true
                    docker run -d --name marshal-ci-pg \
                        -e POSTGRES_USER=marshal -e POSTGRES_PASSWORD=marshal -e POSTGRES_DB=marshal \
                        -p 5433:5432 postgres:16
                    for i in $(seq 1 30); do
                        docker exec marshal-ci-pg pg_isready -U marshal && break
                        sleep 1
                    done
                '''
            }
        }
        stage('vet') {
            steps { sh 'go vet ./...' }
        }
        stage('test') {
            steps {
                sh 'go test ./...'
            }
        }
        stage('determinism') {
            steps {
                sh '''
                    go build -o bin/marshal-sim ./cmd/marshal-sim
                    ./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42 > run1.txt
                    ./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42 > run2.txt
                    cmp run1.txt run2.txt
                '''
            }
        }
        stage('chaos') {
            steps {
                sh '''
                    go build -o bin/marshal-chaos ./cmd/marshal-chaos
                    ./bin/marshal-chaos --seeds 200
                    ./bin/marshal-chaos --seeds 100 --sched backfill
                    ./bin/marshal-chaos --seeds 100 --sched gang
                    ./bin/marshal-chaos --seeds 100 --alloc binpack
                '''
            }
        }
    }

    post {
        always {
            sh 'docker rm -f marshal-ci-pg || true'
        }
    }
}
