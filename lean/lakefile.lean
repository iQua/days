import Lake
open Lake DSL

package daysLean where

lean_lib DaysLean where

@[default_target]
lean_exe dcqcn_check where
  root := `DaysLean.Main
