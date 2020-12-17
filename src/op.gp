set grid
set term pdf font 'Verdana,20'
set output "results/op.pdf"
set datafile separator ","
set key left top
set xtics rotate by 30 right
set style data histogram
set style fill solid border
set ylabel "Time (sec)"

set style histogram clustered
plot newhistogram, 'results/op_trill.csv' u 2:xticlabels(1) t 'Trill' lc 2 fs pattern 1,\
                   'results/op_trill.csv' u 0:2:2 with labels offset -0.5,0.9 font ",14" rotate by 45 notitle,\
                   'results/op_numlib.csv' u 2:xticlabels(1) t 'NumLib' lc 0 fs pattern 2,\
                   'results/op_numlib.csv' u 0:2:2 with labels offset 0.3,0.7 font ",14" rotate by 45 notitle,\
                   'results/op_lifestream.csv' u 2:xticlabels(1) t 'LifeStream' lc 1,\
                   'results/op_lifestream.csv' u 0:2:2 with labels offset 2.2,0.7 font ",14" rotate by 45 notitle
