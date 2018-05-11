(* This benchmark compares partition maps (the latest and greatest version)
   versus a naive approach of allocating arrays. It is intented as a platform
   to investigate different parameters, such as the number of merges or the
   states spaces, as well as the difficulty of the operation (implement
   a [Calculation]), to figure out if partition maps can even beat the naive
   approach. *)

open Printf
open StdLabels
open Test


let () =
  if not !Sys.interactive then begin
    let which = [(*`NaiveList;*) `NaiveArray; `Pmas]  in
    let pars  =
      Test.generate_parameters
        (*~average_number_of_intervals_incr:2.0 *)
        ~domain_sizes:[100; 500; 1000; 5000]
        ~number_of_states:[100; 500; 1000; 5000;]
        ()
    in
    let open Core_bench.Std in
    let m g =
      List.map pars ~f:(fun p ->
        g which p
        |> List.map ~f:(fun (name, f) -> Bench.Test.create ~name f))
      |> List.concat
    in
    let tests = List.concat
      [ m IntBenchmarks.generate_tests
      (*; m FloatBenchmarks.generate_tests *)
      ; m FloatVectorBenchmarks.generate_tests
      ]
    in
    printf "All results are equal\n%!";
    Core.Command.run (Bench.make_command tests)
  end
