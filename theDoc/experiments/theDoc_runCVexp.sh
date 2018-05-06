end=$((SECONDS+(60*60*24*5)))

while [ $SECONDS -lt $end ]; do
    python theDoc_CVexp_7.py
done