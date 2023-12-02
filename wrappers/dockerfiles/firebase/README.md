## Start Firebase Emulator

ref: https://github.com/AndreySenov/firebase-tools-docker

```bash
cd wrappers

docker run \
  --name "firebase-emu" \
  --rm \
  -p=9000:9000 \
  -p=8080:8080 \
  -p=4000:4000 \
  -p=9099:9099 \
  -p=8085:8085 \
  -p=5001:5001 \
  -p=9199:9199 \
  -v $PWD/dockerfiles/firebase:/home/node \
  andreysenov/firebase-tools:11.24.1-node-14-alpine \
  firebase emulators:start --project supa --only auth,firestore --import=/home/node/baseline-data
```

## Export Firebase Data

```bash
docker exec -it "firebase-emu" bash

firebase emulators:export ./baseline-data --project supa
```
