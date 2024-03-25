#!/bin/sh
cd `dirname ${0}`
VERSION=0.10.5
BEFORE=refs/tags/v${VERSION}
AFTER=remotes/trocco/fix/nstring_to_json_to_origin
git checkout master
git branch -D __BEFORE__ > /dev/null 2>&1
git branch -D __AFTER__ > /dev/null 2>&1
git checkout -b __BEFORE__ ${BEFORE}
git checkout -b __AFTER__ ${AFTER}
MAVEN2=https://repo1.maven.org/maven2
TYPES='
mysql
postgresql
redshift
sqlserver
'
for TYPE in ${TYPES}; do
OUT=test/${TYPE}.out
IN=test/${TYPE}.in
. test/${TYPE}.env
if [ "${SSH}" = "true" ]; then
SSH_HOST=
SSH_PORT=22
SSH_USER=
SSH_KEY=test/ssh_key
ssh -N -i ${SSH_KEY} -L ${PORT}:${HOST}:${PORT} ssh://${SSH_USER}@${SSH_HOST}:${SSH_PORT} &
SSH_PID=$!
HOST=localhost
fi
for BRANCH in __BEFORE__ __AFTER__; do
git checkout ${BRANCH}
if [ -f test/${TYPE}.patch ]; then git apply test/${TYPE}.patch; fi
rm -rf embulk-output-${TYPE}/build
./gradlew :embulk-output-${TYPE}:gem
embulk gem uninstall embulk-output-${TYPE}
embulk gem install embulk-output-${TYPE}/build/gems/embulk-output-${TYPE}-${VERSION}-java.gem
if [ -n "${DRIVER}" ]; then DRIVER_PATHS="null test/${DRIVER}"; else DRIVER_PATHS=null; fi
for DRIVER_PATH in ${DRIVER_PATHS}; do
if [ "${DRIVER_PATH}" != "null" ]; then LOG=test/${TYPE}.${BRANCH}.${DRIVER_VERSION}.log; else LOG=test/${TYPE}.${BRANCH}.log; fi
> ${LOG}
cat << EOD > .config.yml
in:
  type: file
  path_prefix: test.jsonl
  parser:
    type: jsonl
    columns:
    - {name: test_boolean, type: boolean}
    - {name: test_long, type: long}
    - {name: test_double, type: double}
    - {name: test_string, type: string}
    - {name: test_timestamp, type: timestamp}
    - {name: test_json, type: json}
    - {name: test_json_text, type: json}
    - {name: test_json_string, type: json}
    - {name: test_json_nstring, type: json}
out:
  type: ${TYPE}
  mode: replace
  host: ${HOST}
  user: ${USER}
  password: '${PASSWORD}'
  database: ${DATABASE}
  schema: ${SCHEMA}
  table: ${TABLE}
  column_options:
    test_timestamp: {type: ${TIMESTAMP_TYPE}}
    test_json_text: {type: TEXT}
    test_json_string: {type: '${JSON_STRING_TYPE}', value_type: string}
    test_json_nstring: {type: '${JSON_NSTRING_TYPE}', value_type: nstring}
  driver_path: ${DRIVER_PATH}
`cat ${OUT}`
EOD
cat .config.yml | tee -a ${LOG}
embulk run .config.yml | tee -a ${LOG}
cat << EOD > .config.yml
in:
  type: ${TYPE}
  host: ${HOST}
  user: ${USER}
  password: '${PASSWORD}'
  database: ${DATABASE}
  column_options:
    test_timestamp: {type: string, timestamp_format: '%Y-%m-%d %H:%M:%S.%6N %z'}
    test_json: {type: json}
    test_json_text: {type: json}
    test_json_string: {type: json}
    test_json_nstring: {type: json}
`cat ${IN}`
out:
  type: command
  command: 'cat -'
  formatter:
    type: jsonl
EOD
cat .config.yml | tee -a ${LOG}
embulk run .config.yml | tee -a ${LOG}
done
if [ -f test/${TYPE}.patch ]; then git apply --reverse test/${TYPE}.patch; fi
rm -f .config.yml
done
if [ -n "${SSH_PID}" ]; then kill -9 ${SSH_PID}; fi
SSH_PID=
rm -f ${IN}
rm -f ${OUT}
done
git checkout master
git branch -D __AFTER__
git branch -D __BEFORE__
