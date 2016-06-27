(ns geo-window.hexbin
  "Normalize points to their hexbin centers")

;;; Adapted from d3.hexbin: https://github.com/d3/d3-hexbin/blob/master/src/hexbin.js

(let [thirdPi (/ Math/PI 3.0)
      angles (mapv #(* % thirdPi) (range 6))
      point (fn [pi pj dx dy]
              [(* (+ pi (/ (bit-and pj 1) 2)) dx)
               (* pj dy)])]
  (defn hexbinner
    "Return a function that takes an x,y point and returns the x,y coordinates of the center of the
  corresponding hexbin"
    [r [x0 y0]]
    (let [dx (* 2 r (Math/sin thirdPi))
          dy (* r 1.5)]
      (fn [[x y]]
        (let [py (/ y dy)
              pj (Math/round py)
              px (- (/ x dx) (/ (bit-and pj 1) 2.0))
              pi (Math/round px)
              py1 (- py pj)]
          (if (> (* (Math/abs py1) 3) 1)
            (let [px1 (- px pi)
                  pi2 (+ pi (/ (if (< px pi) -1 1) 2.0))
                  pj2 (+ pj    (if (< py pj) -1 1))
                  px2 (- px pi2)
                  py2 (- py pj2)]
              (if (> (+ (* px1 px1) (* py1 py1))
                     (+ (* px2 px2) (* py2 py2)))
                (let [pi (+ pi2 (/ (if (bit-and pj 1) 1 -1) 2.0))
                      pj pj2]
                  (point pi pj dx dy))
                (point pi pj dx dy)))
            (point pi pj dx dy)))))))
