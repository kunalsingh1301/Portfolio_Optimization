=LET(
  w1,IFERROR(FILTER('25 - 29 Nov'!$C$2:$C$5000,(TRIM('25 - 29 Nov'!$E$2:$E$5000)="RM")*('25 - 29 Nov'!$C$2:$C$5000<>"")),""),
  w2,IFERROR(FILTER('18 - 22 Nov'!$C$2:$C$5000,(TRIM('18 - 22 Nov'!$E$2:$E$5000)="RM")*('18 - 22 Nov'!$C$2:$C$5000<>"")),""),
  w3,IFERROR(FILTER('11 - 15 Nov'!$C$2:$C$5000,(TRIM('11 - 15 Nov'!$E$2:$E$5000)="RM")*('11 - 15 Nov'!$C$2:$C$5000<>"")),""),
  w4,IFERROR(FILTER('04 - 08 Nov'!$C$2:$C$5000,(TRIM('04 - 08 Nov'!$E$2:$E$5000)="RM")*('04 - 08 Nov'!$C$2:$C$5000<>"")),""),
  w5,IFERROR(FILTER('28 Oct - 01 Nov'!$C$2:$C$5000,(TRIM('28 Oct - 01 Nov'!$E$2:$E$5000)="RM")*('28 Oct - 01 Nov'!$C$2:$C$5000<>"")),""),
  SORT(UNIQUE(VSTACK(w1,w2,w3,w4,w5)))
)



