end=$((SECONDS+24800))

while [ $SECONDS -lt $end ]; do
    python theDoc_CVexp_7.py
done