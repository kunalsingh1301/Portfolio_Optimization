=LET(
  w1, FILTER('Wk1'!$C:$C, 'Wk1'!$E:$E="RM"),
  w2, FILTER('Wk2'!$C:$C, 'Wk2'!$E:$E="RM"),
  w3, FILTER('Wk3'!$C:$C, 'Wk3'!$E:$E="RM"),
  w4, FILTER('Wk4'!$C:$C, 'Wk4'!$E:$E="RM"),
  w5, FILTER('Wk5'!$C:$C, 'Wk5'!$E:$E="RM"),
  SORT(UNIQUE(VSTACK(w1,w2,w3,w4,w5)))
)
