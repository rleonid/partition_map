(* This benchmark compares different, historical, versions of partion maps
   to track improvement and guide development. *)

open Printf
open StdLabels
open Test

let () =
  if not !Sys.interactive then begin
    let which = [`BvAssocs; `Pmas]  in
    let pars  = Test.default_parameters in
    let int_tests = IntBenchmarks.generate_tests which pars in
    let floatv_tests = FloatVectorBenchmarks.generate_tests which pars in
    printf "All results are equal\n%!";
    let open Core_bench.Std in
    Core.Command.run (Bench.make_command
      (List.concat
        [ List.map int_tests ~f:(fun (name, f) -> Bench.Test.create ~name f)
        ; List.map floatv_tests ~f:(fun (name, f) -> Bench.Test.create ~name f)
        ]))
  end
