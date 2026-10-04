#!/usr/bin/env bash
# Shards of CI's debug test matrix (the `test-shard` jobs in .github/workflows/ci.yml).
#
#   .github/test-shards.sh plan          check the layout; print the shard names as a JSON array,
#                                        and each target's shard and each shard's commands to stderr
#   .github/test-shards.sh list          check the layout; print "shard package target" per target
#   .github/test-shards.sh run <shard>   check the layout; run the shard's cargo commands in order
#
# Every mode first checks the layout below against `cargo metadata`: each target that
# `cargo test -p <package> <flags>` builds or runs (lib and bin unit tests, integration tests,
# examples, benches marked `test = true`, doctests) must belong to exactly one shard, and every
# workspace member must be either tested here or listed as untested. A new test file therefore
# lands in its package's whole-package or `rest` unit; the check fails on a target in no shard or
# in two, on a test target name that does not exist, and on a new workspace member.
#
# Layout lines:
#   package <name> [<cargo test flags>...]   a tested package and the flags of its old CI line
#   untested <name>...                       workspace members CI does not test
#   shard <name> <unit>...                   one CI job; units run in the order given
# Units:
#   <package>          the package's whole default selection: `cargo test -p <package> <flags>`
#   <package>:<test>   one integration test target: `cargo test -p <package> <flags> --test <test>`
#   <package>:rest     every target of the package not named in a `<package>:<test>` unit. It
#                      first runs the package's default build (`--no-run`), so it compiles every
#                      example and test target exactly as the unsharded command did, then runs
#                      the remaining targets, then the doctests (`--doc` cannot be combined with
#                      other target selections).
# Every test command passes `-- --show-output`, as the unsharded commands did.
#
# Balance (debug, from days-gpu evidence/P16/cisplit): the named targets are the slowest ones;
# keep `rest`, where new test files land, the lightest shard.
set -euo pipefail

LAYOUT='
package days-executor
package days --features test
package days-legacy --features test
package days-validation --features test
untested xtask

shard lowering-budget days:collective_lowering_budget
shard width-via-load days:width_via_load_fixtures days:p14_dcqcn_pfc_fixtures
shard roce days:p15_roce_fixtures days:p15_roce_collectives
shard validation days-validation
shard rest days:rest days-executor days-legacy
'

# Resolves the layout against the metadata on stdin into
#   {errors: [...], shards: [{name, commands: [[arg...]...]}], coverage: [[shard, package, target]...]}
# shellcheck disable=SC2016 # jq program, not shell.
RESOLVE='
def words: [splits(" +")] | map(select(. != ""));
def libkind: IN("lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro");

# The targets `cargo test -p <package>` builds or runs, each with the arguments that select it
# for running (empty for an example that is only built).
def default_targets:
  .name as $p
  | .targets[]
  | if ((.["required-features"] // []) | length) > 0 then
      {err: "\($p): target \(.name) has required-features; teach .github/test-shards.sh how cargo test treats it"}
    elif (.kind | any(libkind)) then
      (if .test then {id: "lib", run: ["--lib"]} else empty end),
      (if .doctest then {id: "doc", run: []} else empty end)
    elif .kind == ["bin"] then
      if .test then {id: "bin:\(.name)", run: ["--bin", .name]} else empty end
    elif .kind == ["test"] then
      if .test then {id: "test:\(.name)", run: ["--test", .name]} else empty end
    elif .kind == ["bench"] then
      if .test then {id: "bench:\(.name)", run: ["--bench", .name]} else empty end
    elif .kind == ["example"] then
      {id: "example:\(.name)", run: (if .test then ["--example", .name] else [] end)}
    elif .kind == ["custom-build"] then
      empty
    else
      {err: "\($p): target \(.name) has unknown kind \(.kind | join(","))"}
    end;

([$layout | splits("\n") | words | select(length > 0)]) as $rows
| ([$rows[] | select(.[0] == "package") | {key: .[1], value: .[2:]}] | from_entries) as $flags
| ([$rows[] | select(.[0] == "untested") | .[1:][]]) as $untested
| ([$rows[] | select(.[0] == "shard") | {name: .[1], units: .[2:]}]) as $shards
| ([.packages[] | default_targets | select(.err) | .err]) as $target_errors
| ([.packages[] | {key: .name, value: [default_targets | select(.err | not)]}] | from_entries) as $targets
| ([.packages[].name]) as $members
| ([$shards[].units[] | select(contains(":") and (endswith(":rest") | not))]) as $named
| def expand($unit):
    ($unit | split(":")) as $u
    | $u[0] as $p
    | if ($flags | has($p) | not) then
        {err: "unit \($unit): \($p) is not a tested package"}
      elif ($u | length) == 1 then
        {targets: [$targets[$p][].id],
         commands: [["cargo", "test", "-p", $p] + $flags[$p] + ["--", "--show-output"]]}
      elif $u[1] == "rest" then
        ([$named[] | select(startswith($p + ":")) | "test:" + split(":")[1]]) as $taken
        | [$targets[$p][] | select(.id as $id | $taken | index($id) | not)] as $rest
        | ([$rest[] | select(.id != "doc") | .run[]]) as $run
        | {targets: [$rest[].id],
           commands: (
             [["cargo", "test", "-p", $p] + $flags[$p] + ["--no-run"]]
             + (if ($run | length) > 0
                then [["cargo", "test", "-p", $p] + $flags[$p] + $run + ["--", "--show-output"]]
                else [] end)
             + (if ($rest | any(.id == "doc"))
                then [["cargo", "test", "-p", $p] + $flags[$p] + ["--doc", "--", "--show-output"]]
                else [] end))}
      elif ([$targets[$p][].id] | index("test:" + $u[1])) == null then
        {err: "unit \($unit): \($p) has no integration test target \($u[1])"}
      else
        {targets: ["test:" + $u[1]],
         commands: [["cargo", "test", "-p", $p] + $flags[$p] + ["--test", $u[1], "--", "--show-output"]]}
      end;
  ([$shards[] | .name as $s | .units[] | expand(.) as $e
    | if $e.err then {err: $e.err} else {shard: $s, unit: ., e: $e} end]) as $expanded
| ([$expanded[] | select(.err == null) | .shard as $s | (.unit | split(":")[0]) as $p
    | .e.targets[] | [$s, $p, .]]) as $coverage
| ([$targets | to_entries[] | select(.key as $p | $flags | has($p)) | .key as $p
    | .value[] | [$p, .id]]) as $all
| {
    errors: (
      $target_errors
      + [$expanded[] | select(.err) | .err]
      + [$members[] | select(. as $m | ($flags | has($m)) or ($untested | index($m)) | not)
         | "workspace member \(.) is in no shard and not listed as untested"]
      + [($flags | keys[]), $untested[] | select(. as $m | $members | index($m) | not)
         | "\(.) is in the layout but is not a workspace member"]
      + [$untested[] | select(. as $m | $flags | has($m)) | "\(.) is both tested and untested"]
      + [$shards[] | select((.name | test("^[a-z0-9-]+$") | not) or (.units | length) == 0)
         | "shard \(.name // "") needs a lowercase name and at least one unit"]
      + [$shards | group_by(.name)[] | select(length > 1) | "shard \(.[0].name) is defined twice"]
      + (if ($shards | length) == 0 then ["the layout defines no shard"] else [] end)
      + [$all[] as [$p, $t]
         | [$coverage[] | select(.[1] == $p and .[2] == $t) | .[0]] as $in
         | select(($in | length) != 1)
         | if ($in | length) == 0 then "\($p) \($t) is in no shard"
           else "\($p) \($t) is in more than one shard unit: \($in | join(", "))" end]
    ),
    shards: [$shards[] | .name as $s
      | {name: $s, commands: [$expanded[] | select(.shard == $s) | .e.commands[]]}],
    coverage: $coverage
  }
'

resolve() {
    local metadata
    metadata=$(cargo metadata --no-deps --format-version 1 --locked)
    jq --arg layout "$LAYOUT" "$RESOLVE" <<< "$metadata"
}

resolved=$(resolve)
errors=$(jq -r '.errors[]' <<< "$resolved")
if [ -n "$errors" ]; then
    while IFS= read -r error; do
        echo "::error::test shards: $error" >&2
    done <<< "$errors"
    exit 1
fi

case "${1:-}" in
    plan)
        jq -r '.coverage[] | @tsv' <<< "$resolved" >&2
        jq -r '.shards[] | .name as $s | .commands[] | "\($s): \(@sh)"' <<< "$resolved" >&2
        jq -c '[.shards[].name]' <<< "$resolved"
        ;;
    list)
        jq -r '.coverage[] | @tsv' <<< "$resolved"
        ;;
    run)
        shard=${2:?usage: test-shards.sh run <shard>}
        commands=$(jq -r --arg s "$shard" '.shards[] | select(.name == $s) | .commands[] | @sh' \
            <<< "$resolved")
        if [ -z "$commands" ]; then
            echo "::error::test shards: no shard named $shard" >&2
            exit 1
        fi
        # Read every command before running any, so a test reading stdin cannot consume them.
        lines=()
        while IFS= read -r command; do
            lines+=("$command")
        done <<< "$commands"
        for command in "${lines[@]}"; do
            echo "+ $command"
            eval "$command"
        done
        ;;
    *)
        echo "usage: test-shards.sh plan | list | run <shard>" >&2
        exit 2
        ;;
esac
