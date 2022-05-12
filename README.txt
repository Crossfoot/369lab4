Whelp. Theres both a lot and a little to say about this.
First, what a disgusting program, should I spend more than a few hours on this I'd probably
whiteboard it out so I can get a visual of what I'm doing.
Second, I'm glad we're switching to SPARK finally.

Detailing my program, for the first prompt, I did a reduce sided join. No
real reason why I picked it other that that's what was used in your example.
From there I ran it through a second job where each country's counts were summed up.
Finally it ran through a third job that sorted the countries.
Job 1: Input <______, line>; Output <Country, count>
Job 2: Input <Country, count>; Output <Country, sum>
Job 3: Input <Country, sum>; Output (sorted) <Country, sum>

For the second prompt, I did a reduce side join again. I then ran the result of that through a 
second job where I summed up the counts. Finally I ran that through a third job that partitioned
the results based on country and then sorted them by count.
Job 1: Input <_____, line>; Output <Country, count, url>
Job 2: Input <Country, count, url>; Output <Country, url, sum>
Job 3: Input <Country, url, sum>; Output (sorted) <Country, url, sum>

