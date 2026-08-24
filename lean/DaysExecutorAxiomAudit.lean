import DaysExecutor
import Lean.Elab.Command
import Lean.Util.CollectAxioms

/-!
Reproducible trust-base audit for the five DaysExecutor theorem families. `collectAxioms` is the
kernel-dependency traversal used by `#print axioms`; invoking it for every declaration in the
transitive DaysExecutor closure also covers declarations whose private names cannot be written in
a source-level print command.
-/

open Lean Lean.Elab

namespace DaysExecutorAxiomAudit

structure WalkState where
  visited : NameSet := NameSet.empty
  declarations : Array Name := #[]

abbrev WalkM := StateM WalkState

partial def walkDeclaration (env : Environment) (name : Name) : WalkM Unit := do
  let state ← get
  unless state.visited.contains name do
    modify fun current =>
      { visited := current.visited.insert name
        declarations := current.declarations.push name }
    let walkExpr (expr : Expr) : WalkM Unit :=
      expr.getUsedConstants.forM (walkDeclaration env)
    match env.checked.get.find? name with
    | some (.axiomInfo value) => walkExpr value.type
    | some (.defnInfo value) => walkExpr value.type *> walkExpr value.value
    | some (.thmInfo value) => walkExpr value.type *> walkExpr value.value
    | some (.opaqueInfo value) => walkExpr value.type *> walkExpr value.value
    | some (.ctorInfo value) => walkExpr value.type
    | some (.recInfo value) => walkExpr value.type
    | some (.inductInfo value) =>
        walkExpr value.type
        value.ctors.forM (walkDeclaration env)
    | some (.quotInfo _) | none => pure ()

def roots : Array Name := #[
  ``DaysExecutor.f1RemoteLowerBound_proved,
  ``DaysExecutor.f2RoundSerializability_proved,
  ``DaysExecutor.f3RunComposition_proved,
  ``DaysExecutor.f4DecisionPointScope_proved,
  ``DaysExecutor.f5IntraRoundReordering_proved
]

def isDaysExecutorDeclaration (name : Name) : Bool :=
  name.toString.startsWith "DaysExecutor." ||
    name.toString.startsWith "_private.DaysExecutor."

def declarationKind (env : Environment) (name : Name) : String :=
  match env.checked.get.find? name with
  | some (.axiomInfo _) => "axiom"
  | some (.defnInfo _) => "def"
  | some (.thmInfo _) => "theorem"
  | some (.opaqueInfo _) => "opaque"
  | some (.ctorInfo _) => "constructor"
  | some (.recInfo _) => "recursor"
  | some (.inductInfo _) => "inductive"
  | some (.quotInfo _) => "quotient"
  | none => "missing"

run_cmd do
  let env ← getEnv

  for root in roots do
    let (_, state) := (walkDeclaration env root).run {}
    let declarations := state.declarations.filter isDaysExecutorDeclaration
    logInfo m!"ROOT_COUNT root={root} declarations={declarations.size}"

  let (_, state) := (roots.forM (walkDeclaration env)).run {}
  let declarations :=
    state.declarations.filter isDaysExecutorDeclaration |>.qsort Name.lt

  let mut axiomDecls := 0
  let mut defDecls := 0
  let mut theoremDecls := 0
  let mut opaqueDecls := 0
  let mut constructorDecls := 0
  let mut recursorDecls := 0
  let mut inductiveDecls := 0
  let mut quotientDecls := 0
  let mut missingDecls := 0
  let mut modules := NameSet.empty
  for name in declarations do
    match env.checked.get.find? name with
    | some (.axiomInfo _) => axiomDecls := axiomDecls + 1
    | some (.defnInfo _) => defDecls := defDecls + 1
    | some (.thmInfo _) => theoremDecls := theoremDecls + 1
    | some (.opaqueInfo _) => opaqueDecls := opaqueDecls + 1
    | some (.ctorInfo _) => constructorDecls := constructorDecls + 1
    | some (.recInfo _) => recursorDecls := recursorDecls + 1
    | some (.inductInfo _) => inductiveDecls := inductiveDecls + 1
    | some (.quotInfo _) => quotientDecls := quotientDecls + 1
    | none => missingDecls := missingDecls + 1
    if let some moduleIdx := env.getModuleIdxFor? name then
      if let some moduleName := env.header.moduleNames[moduleIdx.toNat]? then
        modules := modules.insert moduleName

  logInfo m!"KIND_COUNTS theorem={theoremDecls} def={defDecls} inductive={inductiveDecls} \
    constructor={constructorDecls} recursor={recursorDecls} axiom={axiomDecls} \
    opaque={opaqueDecls} quotient={quotientDecls} missing={missingDecls}"
  let sortedModules := modules.toArray.qsort Name.lt
  logInfo m!"MODULE_COUNT modules={sortedModules.size}"
  logInfo m!"MODULES {sortedModules.toList}"

  let allowed := NameSet.empty
    |>.insert ``propext
    |>.insert ``Quot.sound
    |>.insert ``Classical.choice
  let mut axiomUnion := NameSet.empty
  let mut auditRows : Array (Name × Array Name) := #[]
  for name in declarations do
    let declarationAxioms ← collectAxioms name
    let sortedAxioms := declarationAxioms.qsort Name.lt
    auditRows := auditRows.push (name, sortedAxioms)
    for usedAxiom in sortedAxioms do
      axiomUnion := axiomUnion.insert usedAxiom
      unless allowed.contains usedAxiom do
        throwError m!"FORBIDDEN_AXIOM declaration={name} axiom={usedAxiom}; \
          allowed=[propext, Quot.sound, Classical.choice]"

  for (name, declarationAxioms) in auditRows do
    logInfo m!"DECLARATION kind={declarationKind env name} name={name} \
      axioms={declarationAxioms.toList}"

  let sortedAxiomUnion := axiomUnion.toArray.qsort Name.lt
  logInfo m!"AXIOM_UNION {sortedAxiomUnion.toList}"
  logInfo m!"PASS audited_declarations={declarations.size}"

end DaysExecutorAxiomAudit
