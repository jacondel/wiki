

gunzip -c enwiki-latest-page.sql.gz | sed s/"),("/"\n"/g | grep "^[0-9]*,0,.*" | grep -oP "^[0-9]*,0,'.*?','" | sed s/,\'$// | sed s/,0,/'\t'/ > titles.tsv

gunzip -c enwiki-latest-pagelinks.sql.gz | sed s/"),("/"\n"/g  | grep "^[0-9]*,0,.*" |  grep -oP "^[0-9]*,0,'.*?',0" | sed s/,0$// | sed s/,0,/'\t'/ > pagelinksWithTitle.tsv #64m23s

gunzip -c enwiki-latest-redirect.sql.gz | sed s/"),("/"\n"/g | grep "^[0-9]*,0,.*" | grep -oP "^[0-9]*,0,'.*?','" | sed s/,\'$// | sed s/,0,/'\t'/ > redirectWithTitle.tsv

#Get rid of titles
#Preface all with LC_ALL=C

LC_ALL=C sort -k2 titles.tsv > titlesSorted.tsv #2m7s
LC_ALL=C sort -k2 pagelinksWithTitle.tsv > pagelinksWithTitleSorted.tsv #80m56s
LC_ALL=C sort -k2 redirectWithTitle.tsv > redirectWithTitleSorted.tsv #0m23s

LC_ALL=C join -o 1.1,2.1 -1 2 -2 2 pagelinksWithTitleSorted.tsv titlesSorted.tsv > links.tsv #26m20s
LC_ALL=C join -o 1.1,2.1 -1 2 -2 2 redirectWithTitleSorted.tsv titlesSorted.tsv > redirect.tsv #1m48s


#Merge redirect into links

LC_ALL=C sort -k2 links.tsv > linksSorted.tsv #6m32s 
LC_ALL=C sort -k1 redirect.tsv > redirectSorted.tsv #0m8s


#JOIN pagelink and redirect 
LC_ALL=C join -t $'\t' -o 1.1,2.2 -1 2 -2 1 linksSorted.tsv redirectSorted.tsv > a.tsv #1m2s
#UNMATCHED BY JOIN
LC_AL=C join -t $'\t' -v1 -1 2 -2 1 -o 1.1,1.2 linksSorted.tsv redirectSorted.tsv > b.tsv #2m22s



#GROUP BY ID
time LC_ALL=C cat a.tsv b.tsv | LC_ALL=C  sort -t " " -k1 | pv |LC_ALL=C awk '($1 == last){sum = sum","$2} 
     (last == ""){sum = $2}
     ($1 != last) && (last != "") {print last "," sum; sum = $2}
                                  {last = $1}
     END                          {print last "," sum}' > res.csv #32m28s


