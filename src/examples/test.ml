(* Definition of terms:

   A partition map is a way to represent a function, an association, a map.

   Parameters - These are the keys of the partition map; the set of things for
   which we want to keep track of some association. In our case the set are
   the intergers in [0,n) (n = parameter_size ).
*)

open Util
open Printf
open StdLabels

type parameters =
(* The number of elements in universe, the parameters over which we want to
   compute some relation. *)
  { domain_size : int

(* How many states we test in this example, or how many times we're going to
   merge. State in this example, is a map from parameters to some values,
   we are concerned with combining them. *)
  ; number_of_states  : int

(* The variaton in the states that we need to merge.

   Our state is an observation of some distribution over our parameters.
   This is crucial to why we might want to use partition maps, the total
   function should not have some solution ahead of time.

   Each state will have a finite number of unique value.We'll use a poisson
   distribution to determine how many. To keep things bounded, we'll assume
   that there will be 2.5 values on average.
*)
  ; average_number_of_values : float

(* For a given state, once we know the number of values, we have to assign them
   to our parameters, but we want them cluster together. The actual method of
   assigning might be dependent on the actual PM representation, so to keep
   things simple we will generate where the unique values start and stop. But
   the number intervals isn't the same as the number of values as some
   parameters later on might take previous values:

   [1, 10] -> 0, [11, 90] -> 1, [91, 100] -> 0

   So the number of start & stops will also be poisson, but greater.

*)
  ; average_number_of_intervals_incr : float
  }

let parameters_to_string
  { domain_size
  ; number_of_states
  ; average_number_of_values
  ; average_number_of_intervals_incr
  } =
  sprintf "Size of domain: %d\n\
           Number of states to merge: %d\n\
           Average number of values (per state): %f\n\
           Intervals incr: %f"
    domain_size number_of_states
    average_number_of_values average_number_of_intervals_incr

let parameters_to_encoded
  { domain_size
  ; number_of_states
  ; average_number_of_values
  ; average_number_of_intervals_incr
  } =
  sprintf "( %d, %d, %f, %f )"
    domain_size
    number_of_states
    average_number_of_values
    average_number_of_intervals_incr

let default_parameters =
  { domain_size = 1000
  ; number_of_states = 1000
  ; average_number_of_values = 1.5
  ; average_number_of_intervals_incr = 1.0
  }

let generate_parameters
    ?average_number_of_values
    ?average_number_of_intervals_incr
    ~domain_sizes ~number_of_states ()
  =
  let anv =
    match average_number_of_values with
    | None -> default_parameters.average_number_of_values
    | Some a -> a
  in
  let ani =
    match average_number_of_intervals_incr with
    | None -> default_parameters.average_number_of_intervals_incr
    | Some a -> a
  in
  List.map domain_sizes ~f:(fun ds ->
    List.map number_of_states ~f:(fun ns ->
      { domain_size = ds
      ; number_of_states = ns
      ; average_number_of_values = anv
      ; average_number_of_intervals_incr = ani
      }))
  |> List.concat

(* Draw from the poisson distribution.

   This is a HUGE assumption about your data. I chose Poisson because in a
   good sense it is the simplest distribution for the support (positive ints)
   that we use in the demo. *)
let poisson lambda =
  if lambda < 0. then
    invalid_argf "Negative lambda: %f" lambda
  else
    let p = exp (~-.lambda) in
    let rec loop u p s x =
      if u <= s then x
      else
        let nx = x + 1 in
        let np = p *. lambda /. (float nx) in
        let ns = s +. np in
        loop u np ns nx
    in
    fun () ->
      let u = Random.float 1. in
      loop u p p 0

let assign_starts_and_stops last_end n =
  let rec loop acc s n =
    if n = 1 then
      List.rev ((s, last_end) :: acc)
    else
      let e = last_end - s - n in
      let stop = s + Random.int e in
      loop ((s, stop) :: acc) (stop + 1) (n - 1)
  in
  loop [] 0 n

module type Calculation = sig
  val description : string
  type t
  val zero : t
  val op : t -> t -> t
  (* How we initialize values. We take positive integer values from the Poisson
     distribution and map them to this data type. *)
  val of_int : int -> t
  val equal : t -> t -> bool
end

module Benchmarks (C : Calculation) = struct

  let random_assignment p =
    let number_of_values = poisson p.average_number_of_values in
    let average_number_of_intervals =
      p.average_number_of_values +. p.average_number_of_intervals_incr
    in
    let number_of_intervals = poisson average_number_of_intervals in
    let last_end = p.domain_size - 1 in
    let nov = 1 + number_of_values () in
    if nov = 1 then
      [(0, last_end), C.zero]
    else
      let noi = 1 + number_of_intervals () in
      let ss = assign_starts_and_stops last_end noi in
      List.mapi ~f:(fun i ss -> ss, C.of_int (i mod nov)) ss

  (*
    How we assign to the intervals can be different, and will not be
    benchmarked....
    After we have these calculations we merge all of them into one final state!
  *)

  let states p =
    Array.init p.number_of_states
      ~f:(fun _ -> random_assignment p)

  (* Using int lists but mapping into an array. *)
  let time_list_merge p list_states =
    let acc = Array.make p.domain_size C.zero in
    fun () ->
      Array.iter list_states ~f:(fun lst ->
        List.iter lst ~f:(fun ((s, e), v) ->
            for i = s to e do
              acc.(i) <- C.op acc.(i) v
            done));
      `Array acc

  (* Using naive matrices. *)

  let states_as_arrays p states =
    Array.map states ~f:(fun lst ->
      let a = Array.make p.domain_size C.zero in
      List.iter lst ~f:(fun ((s, e), v) ->
          for i = s to e do a.(i) <- v done);
      a)

  let time_array_merge p array_states =
    let acc = Array.make p.domain_size C.zero in
    let last_end = p.domain_size - 1 in
    fun () ->
      Array.iter array_states ~f:(fun a ->
        for i = 0 to last_end do
          acc.(i) <- C.op acc.(i) a.(i)
        done);
      `Array acc

  (* Using association lists backed by bitvectors lists. *)

  (* An ugly hack to avoid redesigning the now deprecated bitvector methods. *)
  let size_ref = ref 0

  module Bv_sets = struct

    open Lib08

    type t = Bitvector.t
    let union = Bitvector.union
    let init () = Bitvector.create ~size:(!size_ref) false

    type inter_diff = { intersection : t; difference : t; same : bool; none : bool; }
    let inter_diff t1 t2 =
      let intersection, difference, same, none =
        Bitvector.inter_diff t1 t2 in
      { intersection; difference; same; none}

    let iter_set_indices t ~f =
      Bitvector.iter_true t f

    let complement = Bitvector.negate

    let of_interval (s, e) =
      let bv = init () in
      for i = s to e do Bitvector.set bv i done;
      bv

    let to_string t =
      Bitvector.print Format.std_formatter t;
      Format.flush_str_formatter ()

    end (* Bv_sets *)

  module Bv_assocs = Lib08.Assoc_list.Make(Bv_sets)

  let states_as_bv_assocs p states =
    size_ref := p.domain_size;
    Array.map states ~f:(fun lst ->
      List.map lst ~f:(fun (i, v) -> Bv_sets.of_interval i, v)
      |> Bv_assocs.of_list)

  let time_bv_assoc_merge bv_states =
    let init = Bv_assocs.init_everything C.zero in
    fun () ->
      `Bv (Array.fold_left bv_states ~init ~f:(fun s1 s2 ->
            Bv_assocs.map2 s1 s2 ~f:C.op))

  (* Using partition maps. Pma = Ascending Partition Maps, the default kind.*)
  module Pma = Partition_map.Ascending

  let states_as_pmas states =
    Array.map states ~f:(Pma.of_ascending_interval_list C.equal)

  let time_pmas_merge p pm_states =
    let size = p.domain_size in
    let starting_ascending_pm = Pma.init ~size C.zero in
    fun () ->
      `Pma (Array.fold_left pm_states ~init:starting_ascending_pm
          ~f:(fun a p -> Pma.merge C.equal a p C.op))

  let equal_results r1 r2 = match r1, r2 with
    | `Init,    `Array _  -> true
    | `Init,    `Pma _    -> true
    | `Init,    `Bv _     -> true
    | `Array _, `Init     -> true
    | `Pma _,   `Init     -> true
    | `Bv _,    `Init     -> true
    | `Init,    `Init     -> false
    | `Array a, `Pma p    ->
        Pma.fold_indices_and_values p ~init:true
          ~f:(fun e i v -> e && C.equal a.(i) v)
    | `Array a, `Array b  ->
        Array.fold_left a ~init:(0, true) ~f:(fun (i, e) v ->
          (i + 1, e && C.equal v b.(i)))
        |> snd
    | `Array a, `Bv b  ->
        Bv_assocs.fold_values b ~init:true ~f:(fun eq i v ->
          eq && C.equal v a.(i))
    | `Pma p,   `Array a  ->
        Pma.fold_indices_and_values p ~init:true
          ~f:(fun e i v -> e && C.equal a.(i) v)
    | `Pma a,   `Pma b    ->
        Pma.equal C.equal a b
    | `Pma a,   `Bv b     ->
        Bv_assocs.fold_values b ~init:true ~f:(fun eq i v ->
          eq && C.equal v (Pma.get a i))
    | `Bv b1,   `Bv b2    ->
        let c1 = List.sort compare (Bv_assocs.to_list b1) in
        let c2 = List.sort compare (Bv_assocs.to_list b2) in
        c1 = c2   (* Good old polymorphic compare ... *)
    | `Bv b,    `Array a   ->
        Bv_assocs.fold_values b ~init:true ~f:(fun eq i v ->
          eq && C.equal v a.(i))
    | `Bv b,    `Pma a   ->
        Bv_assocs.fold_values b ~init:true ~f:(fun eq i v ->
          eq && C.equal v (Pma.get a i))

  let generate_tests which p =
    let states = states p in
    let name s =
      sprintf "%s of %s %s" s C.description (parameters_to_encoded p)
    in
    let tests =
      List.map which ~f:(function
        | `NaiveList  ->
            name "Naive lists into array"
            , time_list_merge p states
        | `NaiveArray ->
            let arrays = states_as_arrays p states in
            name "Naive two array"
            , time_array_merge p arrays
        | `BvAssocs   ->
            let bvas   = states_as_bv_assocs p states in
            name "Bitvector backed assocs"
            , time_bv_assoc_merge bvas
        | `Pmas       ->
            let pmas   = states_as_pmas states in
            name "Ascending partition maps"
            , time_pmas_merge p pmas)
    in
    let _ignore_last_result =
      List.fold_left tests ~init:`Init
        ~f:(fun pr (name, t) ->
              let r = t () in
              if not (equal_results pr r) then
                invalid_argf "Results do not match, starting with: %s" name
              else
                r)
    in
    tests

end (* Benchmarks *)

module IntBenchmarks =
  Benchmarks (struct
    let description = "Integer values"
    type t = int
    let zero = 0
    let op = ( + )
    let of_int x = x
    let equal (x : int) y = x = y
  end)

module FloatBenchmarks =
  Benchmarks (struct
    let description = "Float values addition"
    type t = float
    let zero = 0.0
    let op = ( +. )
    let of_int = float
    let equal (x : float) y = x = y
  end)

module FloatMBenchmarks =
  Benchmarks (struct
    let description = "Float values multiplication"
    type t = float
    let zero = 1.0
    let op = ( *. )
    let of_int x = float (x + 1)  (* Avoid zero... otherwise PM's have a HUGE advantage. *)
    let equal (x : float) y = x = y
  end)

module IntVector = struct

  let description = "Integer vector"
  type t =
    { x : int
    ; y : int
    ; z : int
    }
  let zero =
    { x = 0
    ; y = 0
    ; z = 0
    }
  let op t1 t2 =
    { x = t1.x + t2.x
    ; y = t1.y + t2.y
    ; z = t1.z + t2.z
    }

  let of_int t =
    let rec r = function
      | 0 -> { x = 1; y = 0; z = 0}
      | 1 -> { x = 0; y = 1; z = 0}
      | 2 -> { x = 0; y = 0; z = 1}
      | n -> op (r (n - 1)) (r (n - 2))
    in
    r t

  let equal t1 t2 =
    t1.x = t2.x && t1.y = t2.y && t1.z = t2.z

end (* IntVector *)

module IntVectorBenchmarks = Benchmarks (IntVector)

module FloatVector = struct

  let description = "Float vector"

  type t =
    { x : float
    ; y : float
    ; z : float
    }

  let zero =
    { x = 0.
    ; y = 0.
    ; z = 0.
    }
  let op t1 t2 =
    { x = t1.x +. t2.x
    ; y = t1.y +. t2.y
    ; z = t1.z +. t2.z
    }

  let of_int t =
    let rec r = function
      | 0 -> { x = 1.; y = 0.; z = 0.}
      | 1 -> { x = 0.; y = 1.; z = 0.}
      | 2 -> { x = 0.; y = 0.; z = 1.}
      | n -> op (r (n - 1)) (r (n - 2))
    in
    r t

  let equal t1 t2 =
    t1.x = t2.x && t1.y = t2.y && t1.z = t2.z

end (* FloatVector *)

module FloatVectorBenchmarks = Benchmarks (FloatVector)


