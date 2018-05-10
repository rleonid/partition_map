(* Deprecated version. *)

open StdLabels
open Printf
open Util

module type S = sig

  type t

  val to_string : t -> string

  val union : t -> t -> t

  val init : unit -> t

  type inter_diff =
    { intersection : t
    ; difference   : t
    ; same         : bool
    ; none         : bool
    }

  val inter_diff : t -> t -> inter_diff

  val iter_set_indices : t -> f:(int -> unit) -> unit

  val complement : t -> t

end (* S *)

module type M = sig

  type 'a t

  type set

  val empty : 'a t

  val to_string_full : ('a -> string) -> 'a t -> string

  val of_list : (set * 'a) list -> 'a t

  val singleton : set -> 'a -> 'a t

  val to_list : 'a t -> (set * 'a) list

  val domain : 'a t -> set

  val length : 'a t -> int

  val add : set -> 'a -> 'a t -> 'a t

  val join : 'a t -> 'a t -> 'a t

  val get : set -> 'a t -> 'a t option

  exception StillMissing of string

  val get_exn : set -> 'a t -> 'a t

  val fold : 'a t -> init:'b -> f:('b -> set -> 'a -> 'b) -> 'b

  (*val iter : 'a t -> f:(set -> 'a -> unit) -> unit *)

  val fold_values : 'a t -> init:'b -> f:('b -> int -> 'a -> 'b) -> 'b

  val iter_values : 'a t -> f:(int -> 'a -> unit) -> unit

  (* Default to not bijective map *)
  val map : ?bijective:bool -> 'a t -> f:('a -> 'b) -> 'b t

  (* Not the perfect name for this function. *)
  val concat_map : 'a t -> f:(set -> 'a -> 'b t) -> 'b t

  val concat_map2 : 'a t -> by:'b t -> f:(set -> 'a -> 'b -> 'c t) -> 'c t

  val concat_map2_partial : 'a t -> by:'b t -> f:(set -> 'a -> 'b -> 'c t) ->
    missing:(set -> 'a -> 'c t) -> 'c t

  val map2 : 'a t -> 'b t -> f:('a -> 'b -> 'c) -> 'c t

  val map3 : 'a t -> 'b t -> 'c t -> f:('a -> 'b -> 'c -> 'd) -> 'd t

  val map4 : 'a t -> 'b t -> 'c t -> 'd t -> f:('a -> 'b -> 'c -> 'd -> 'e) -> 'e t

  val init_everything : 'a -> 'a t

  val map2_partial : 'a t -> by:'b t -> missing:(set -> 'a -> 'c t) ->
    f:('a -> 'b -> 'c) -> 'c t

  val map3_partial : 'a t ->
    by1:'b t -> missing1:(set -> 'a -> 'b t) ->
    by2:'c t -> missing2:(set -> 'a -> 'b -> 'c t) ->
    f:('a -> 'b -> 'c -> 'd) -> 'd t

  val split_map : 'a t -> f:(set -> 'a -> [< `Fst of 'b | `Snd of 'c ]) ->
     'b t * 'c t

end (* M *)

module Make (AS : S) : (M with type set = AS.t) = struct

  type set = AS.t

  type 'a t = (set * 'a) list

  let empty = []

  let to_string t =
    string_of_list t
      ~sep:"\n\t"
      ~f:(fun (s,_v) -> sprintf "%s" (AS.to_string s))

  let to_string_full v_to_s t =
    string_of_list t ~sep:"\n\t"
      ~f:(fun (s,v) -> sprintf "%s:%s" (AS.to_string s) (v_to_s v))

  (* Union, tail recursive. *)
  let mutate_or_add lst ((alleles, value) as p) =
    let rec loop acc = function
      | (s, v) :: t when v = value -> acc @ (AS.union s alleles, v) :: t
      | h :: t                     -> loop (h :: acc) t
      | []                         -> p :: acc
    in
    loop [] lst

  let add alleles v l = mutate_or_add l (alleles,v)

  let join l1 l2 = List.fold_left l1 ~init:l2 ~f:mutate_or_add

  let of_list l = List.fold_left l ~init:[] ~f:mutate_or_add

  let singleton s a = [s,a]

  let to_list l = l

  let domain = function
    | []             -> AS.init ()
    | (init, _) :: t -> List.fold_left t ~init ~f:(fun u (s, _) -> AS.union u s)

  let length = List.length

  exception StillMissing of string

  let still_missingf fmt =
    ksprintf (fun s -> raise (StillMissing s)) fmt

  let set_assoc_exn to_find t =
    let rec loop to_find acc = function
      | []          -> still_missingf "%s after looking in: %s"
                        (AS.to_string to_find) (to_string t)
      | (s, v) :: t ->
          let id = AS.inter_diff to_find s in
          if id.same then begin                             (* Found everything *)
            (to_find, v) :: acc
          end else if id.none then begin                      (* Found nothing. *)
            loop to_find acc t
          end else begin                                    (* Found something. *)
            loop id.difference ((id.intersection, v) :: acc) t
          end
    in
    loop to_find [] t

  let set_assoc to_find t =
    try Some (set_assoc_exn to_find t)
    with (StillMissing _) -> None

  let get_exn = set_assoc_exn

  let get = set_assoc

  (*let iter l ~f = List.iter l ~f:(fun (a, s) -> f a s) *)

  (*let iter_values l ~f =
    List.iter l ~f:(fun (a, v) ->
      AS.iter_set_indices a ~f:(fun i -> f i v)) *)

  let fold l ~init ~f = List.fold_left l ~init ~f:(fun b (s, a) -> f b s a)

  (*let iter l ~f = fold l ~init:() ~f:(fun () (s, a) -> f s a) *)

  let fold_values l ~init ~f =
    let r = ref init in
    List.iter l ~f:(fun (a, v) ->
      AS.iter_set_indices a ~f:(fun i ->
      r := f !r i v));
    !r

  let iter_values l ~f =
    fold_values l ~init:() ~f:(fun () i v -> f i v)

  let set_assoc_with_mg to_find slst ~missing ~g ~init =
    let rec loop to_find acc = function
      | []          -> add to_find (missing to_find) acc
      | (s, v) :: t ->
          let id = AS.inter_diff to_find s in
          if id.same then begin                      (* Found everything *)
            add to_find (g v) acc
          end else if id.none then begin                 (* Found nothing. *)
            loop to_find acc t
          end else begin                                    (* Found something. *)
            let nacc = add id.intersection (g v) acc in
            loop id.difference nacc t
          end
    in
    loop to_find init slst

  let list_map_snd lst ~f =
    List.map lst ~f:(fun (k, v) -> k, f v)

  let map ?bijective l ~f =
    match bijective with
    | Some true         ->                                            (* O(n) *)
      list_map_snd ~f:(fun v -> f v) l
    | Some false | None ->                                          (* O(n^2) *)
      List.fold_left l ~init:[] ~f:(fun acc (s, v) -> add s (f v) acc)

  let empty_if_none = function
    | None    -> ""
    | Some s  -> s

  let set_assoc_k ?n ?missing to_find t ~k ~init =
    let rec loop to_find acc = function
      | []          -> begin match missing with
                       | None -> still_missingf "%s%s after looking in: %s"
                                  (empty_if_none n)
                                  (AS.to_string to_find) (to_string t)
                       | Some m -> m to_find acc
                       end
      | (s, v) :: t ->
          let id = AS.inter_diff to_find s in
          if id.same then begin                           (* Found everything *)
            k to_find v acc
          end else if id.none then begin                    (* Found nothing. *)
            loop to_find acc t
          end else begin                                  (* Found something. *)
            let nacc = k id.intersection v acc in
            loop id.difference nacc t
          end
    in
    loop to_find init t

  let absorb_k t ~init ~f = List.fold_left t ~init ~f

  let absorb t ~init = absorb_k t ~init ~f:mutate_or_add

  let concat_map l ~f =
    List.fold_left l ~init:[] ~f:(fun init (s, a) -> absorb (f s a) ~init)

  (* The order of set arguments matters for performance. Better to fold over
     the longer list and lookup (set_assoc_k) into the shorter one. Remember
     that the lookup requires a Allele.inter_diff per item! Perhaps it makes
     sense to keep track of the length to avoid O(n) lookups and then
     automatically re-order functional arguments as necessary?

     Probably just need a better data structure. *)
  let concat_map2 l ~by ~f =
    (*printf "%d %d\n" (List.length l) (List.length by); *)
    List.fold_left l ~init:[] ~f:(fun init (s, a) ->
      set_assoc_k s by ~init ~k:(fun intersect b init ->
        absorb (f intersect a b) ~init))

  let concat_map2_partial l ~by ~f ~missing =
    List.fold_left l ~init:[] ~f:(fun init (s, a) ->
      set_assoc_k s by ~init
        ~k:(fun intersect b init -> absorb (f intersect a b) ~init)
        ~missing:(fun sm init -> absorb ~init (missing sm a)))

  let map2 l1 l2 ~f =
    List.fold_left l1 ~init:[] ~f:(fun init (s, a) ->
      set_assoc_k s l2 ~init ~k:(fun intersect b acc ->
        mutate_or_add acc (intersect, f a b)))

  let map3 l1 l2 l3 ~f =
    List.fold_left l1 ~init:[] ~f:(fun init (is1, a) ->
      set_assoc_k ~n:"1" is1 l2 ~init ~k:(fun is2 b init ->
        set_assoc_k ~n:"2" is2 l3 ~init ~k:(fun intersect c acc ->
          mutate_or_add acc (intersect, f a b c))))

  let map4 l1 l2 l3 l4 ~f =
    List.fold_left l1 ~init:[] ~f:(fun init (is1, a) ->
      set_assoc_k is1 l2 ~init ~k:(fun is2 b init ->
        set_assoc_k is2 l3 ~init ~k:(fun is3 c init ->
          set_assoc_k is3 l4 ~init ~k:(fun intersect d acc ->
            mutate_or_add acc (intersect, f a b c d)))))

  let map2_partial l ~by ~missing ~f =
    List.fold_left l ~init:[] ~f:(fun init (s, a) ->
      set_assoc_k s by ~init
        ~k:(fun intercept b acc -> mutate_or_add acc (intercept, f a b))
        ~missing:(fun sm init -> absorb ~init (missing sm a)))

  let map3_partial l ~by1 ~missing1 ~by2 ~missing2 ~f =
    List.fold_left l ~init:[] ~f:(fun init (is1, a) ->
      let k is2 b init =
        let k2 intercept c acc = mutate_or_add acc (intercept, f a b c) in
        set_assoc_k is2 by2 ~init ~k:k2
          ~missing:(fun sm init ->
            absorb_k (missing2 sm a b) ~init ~f:(fun init (s, b) -> k2 s b init))
      in
      set_assoc_k is1 by1 ~init ~k
        ~missing:(fun sm init ->
          absorb_k (missing1 sm a) ~init ~f:(fun init (s, b) -> k s b init)))

  let init_everything v =
    let nothing = AS.init () in
    [AS.complement nothing, v]

  let split_map l ~f =
    let rec loop bs cs = function
      | []          -> bs, cs
      | (s, a) :: t ->
          match f s a with
          | `Fst b -> loop ((s, b) :: bs) cs t
          | `Snd c -> loop bs ((s, c) :: cs) t
    in
    loop [] [] l

end (* Make *)
