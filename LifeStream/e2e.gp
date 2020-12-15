set grid
set term pdf font 'Verdana,20'
set key left top
set output "results/e2e.pdf"
set datafile separator ","
set xlabel "Data size (million events)"
set ylabel "Time (sec)"

plot 'results/e2e_trill.csv' u 1:2 w lp t 'Trill' lw 2 pt 5 lc 2, \
     'results/e2e_numlib.csv' u 1:2 w lp t 'NumLib' lw 2 pt 7 lc 0, \
     'results/e2e_lifestream.csv' u 1:2 w lp t 'LifeStream' lw 2 pt 9 lc 1
