Download Link: https://assignmentchef.com/product/solved-stats507-homework-8-mapreduce-hadoop-and-spark
<br>
<h1>1         Warmup: counting words with mrjob</h1>

In this problem, you’ll get a gentle introduction to mrjob and running mrjob on the Fladoop cluster. I have uploaded a large text file to the Fladoop cluster. Your job is to count how many times each word occurs in this file.

<ol>

 <li>Write an mrjob job that takes text as input and counts how many times each word occurs in the text. Your script should strip punctuation like full stops, commas and semicolons, but you may treat hyphens, apostrophes, etc. as you wish. Simplest is to treat, e.g., “John’s” as two words, “John” and “s”, but feel free to do more complicated processing if you wish. Your script should ignore case, so that “Cat” and “cat” are considered the same word. Your output should be a collection of (word,count) pairs. Please save your script in a file called py and include it in your submission.</li>

 <li>To test your code, I have uploaded a simple text file to the course webpage:</li>

</ol>




Download this file and test your code either on your local machine or on the Fladoop grid. The file is small enough that you should be able to check by hand whether your code is behaving correctly. Save the output of running your script on this small file to a file called simple word counts.txt and include it in your submission. <strong>Note: </strong>use the redirect arrow &gt; to send the Hadoop output to a file. This will only send the stdout output to the file, while still printing the Hadoop error/status messages to the terminal.

<ol start="3">

 <li>Once you are confident in the correctness of your program, run your mrjob script on the file hdfs:/var/stat507w19/darwin.txt</li>

</ol>

on the Fladoop grid (this file is the Project Gutenberg plain text version of Charles Darwin’s scientific work <em>On the Origin of Species</em>). Note that this file is on hdfs, not the local file system, so you’ll have to run your script accordingly. Save the output to a file called darwin word counts.txt, and include it in your submission.

<ol start="4">

 <li>Zipf’s law states, roughly, that if one plots word frequency against frequency rank(i.e., most frequent word, second most frequent word, etc.), the resulting line is (approximately) linear on a log-log scale. Using the information in darwin word counts.txt, make a plot of word frequency as a function of word rank on a log-log scale for all words in the file hdfs:/var/stats507w19/darwin.txt</li>

</ol>

Give an appropriate title to your plot and include axis labels. Save the plot as a pdf file called zipf.pdf, and include it in your submission.

<ol start="5">

 <li>How “Zipfian” does the resulting plot look (It suffices for you to state whether ornot your plot looks approximately like a line)? You can read more about Zipf’s law and about power laws generally at the respective Wikipedia pages (<a href="https://en.wikipedia.org/wiki/Zipf's_law">https:// </a><a href="https://en.wikipedia.org/wiki/Zipf's_law">wikipedia.org/wiki/Zipf’s_law</a><a href="https://en.wikipedia.org/wiki/Zipf's_law">,</a> <a href="https://en.wikipedia.org/wiki/Power_law">https://en.wikipedia.org/wiki/Power_ </a><a href="https://en.wikipedia.org/wiki/Power_law">law</a><a href="https://en.wikipedia.org/wiki/Power_law">)</a>. For more about power laws, I recommend this survey paper by Mark Newman, a faculty member here at University of Michigan <a href="https://arxiv.org/pdf/cond-mat/0412004.pdf">https://arxiv.org/pdf/ </a><a href="https://arxiv.org/pdf/cond-mat/0412004.pdf">cond-mat/0412004.pdf</a><a href="https://arxiv.org/pdf/cond-mat/0412004.pdf">.</a></li>

</ol>

<h1>2         Computing Sample Statistics with mrjob</h1>

In this problem, we’ll compile some very basic statistics summarizing a toy dataset. The

file




contains a collection of (class,value) pairs, one per line, with each line taking the form class label,value, where class label is a nonnegative integer and value is a float. Each pair corresponds to an observation, with the class labels corresponding to different populations, and the values corresponding to some measured quantity.

<ol>

 <li>Write a mrjob program called py that takes as input a sequence of (label,value) pairs like in the file a<a href="http://www-personal.umich.edu/~klevin/teaching/Winter2019/STATS507/populations_small.txt">,</a> and outputs a collection of (label, number of samples, mean, variance) 4-tuples, in which one 4-tuple appears for each class label in the data, and the mean and variance are the <em>sample </em>mean and variance, respectively, of all the values for that class label. Thus, if 25 unique class labels are present in the input then your program should output 25 lines, one for each class label. <strong>Note: </strong>I don’t care whether you use <em>n </em>or <em>n</em>−1 in the denominator of your sample variance formula—just be clear which one you are using. <strong>Note: </strong>you don’t need to do any special formatting of the Hadoop output. That is, your output is fine if it consists of lines of the form label [number,mean,variance] or similar.</li>

</ol>

Think carefully about what your key-value pairs should be here, as well as what your mappers, reducers, etc. should be. Should there be more than one step in your job? Sit down with pen and paper first! <strong>Hint: </strong>to compute the sample mean and sample variance of a collection of numbers, it suffices to know their sum, the sum of their squares, and the size of the collection.

Please include a copy of mr_summary_stats.py in your submission.

<ol start="2">

 <li>Download the small file at<a href="http://www-personal.umich.edu/~klevin/teaching/Winter2019/STATS507/populations_small.txt">t</a><a href="http://www-personal.umich.edu/~klevin/teaching/Winter2019/STATS507/populations_small.txt">.</a> Run your mrjob script on this file, either on your local machine or on Fladoop, and write the output to a file called summary small.txt. Please include this file in your submission. Inspect your program’s output and verify that it is behaving as expected.</li>

 <li>I have uploaded to the Fladoop cluster a much larger data file, located on theHDFS file system at hdfs:/var/stats507w19/populations txt. Once you are <em>sure </em>that your script is doing what you want, run it on this file. Be sure to use the -r hadoop command to tell mrjob to run on the Hadoop server rather than on the login node. Save the output to a file called summary large.txt. Download this file and include it in your submission. Please also include in your notebook file a copy-paste of your shell session on Fladoop in a markdown cell (i.e., a cell that will display as code but will not be executed by the interpreter).</li>

 <li>Use matplotlib and the results in summary large.txt to create a plot displaying 95% confidence intervals for the sample means of the populations given by the class labels in file hdfs:/var/stats507w19/populations large.txt. You will probably want to make a boxplot for this, but feel free to get creative if you think you have a better way to display the information. Make sure your plot has a sensible title and axis labels. Save your plot as a pdf called pdf and include it in your submission.</li>

</ol>

<h1>3         Graph Processing: Counting Triangles with PySpark</h1>

A classic task in graph processing is called “triangle counting”. If you have never heard of graphs, that’s okay! It suffices to know that a graph is a set of <em>nodes </em>(also called <em>vertices</em>), pairs of which are joined by <em>edges </em>(see <a href="https://en.wikipedia.org/wiki/Graph_theory">https://en.wikipedia.org/wiki/Graph_theory </a>for more). A <em>triangle </em>in graph theory is a set of three nodes, say {<em>a,b,c</em>}, such that all three nodes are joined by edges. Triangle counting is closely related to a fundamental task for social media companies, who may wish to suggest new “friends” to users based on their existing social network. In this problem, you’ll implement triangle counting in the MapReduce framework using PySpark. We should note that in practice, the MapReduce framework is rather poorly-suited to the problem of counting triangles, but it’s a good problem to get you practice with the framework, so we’ll leave that be.

The input for this problem will be a collection of files representing users’ friend lists in a social network. Each user in the network is assigned a numeric ID, and that user’s friend list is contained in a file called n.txt, where n is the user’s ID. Each such file contains a single space-separated line, of the form

n f1 f2 … fK

where n is the node and f1,f2,…,fK are the IDs of the friends of n. So, if node 1 is friends with nodes 2,5 and 6, there will be a file 1.txt, containing only the line 1 2 5 6. If node 10 has no friends, then there will be a file 10.txt, containing only the line 10, or perhaps no file at all. Note that just because an ID appears in a friend list, that doesn’t necessarily mean that there will be a file listing that user’s friends, but you may assume (1) <strong>symmetry: </strong>if 100 is a friend of 200, then 200 is a friend of 100. (2) <strong>no duplication: </strong>each friend appears in a given friend list at most once (i.e., every file will contain a given number at most once).

Once again, before you dive in and write a bunch of code, sit down and think about the problem. What is the right “fundamental unit” of the problem? What should your keys and values look like? <strong>Hint: </strong>the simplest solution to this problem involves multiple steps, involving a standard map-reduce pattern and a subsequent filtering operation. As usual, overly complicated solutions will not receive full credit.

<ol>

 <li>Write a PySpark job that takes the described input and produces a list of all thetriangles in the network, one per line. Each triangle should be listed as a spaceseparated line node1 node2 node3, with the entries sorted numerically in ascending order. So, if nodes 2, 5 and 15 form a triangle, the output should include the triple (2,5,15), but <em>not </em>(2,15,5), (15,2,5), etc. Save your script in a file called py and include it in your submission.</li>

 <li>Test your script on the set of 5 simple files in the HDFS directory</li>

</ol>

hdfs:/var/stats507w19/fof/friends.simple

which is small enough that you should be able to work out by hand what the correct output is. How many triangles are there? List them in a file called small triangle list.txt and include it in your submission.

<ol start="3">

 <li>Once you are confident that your script is correct, run it on the larger data set, storedon HDFS at hdfs:/var/stats507w19/fof/friends1000 Save the list of triangles to a file called big triangle txt, and include it in your submission. Don’t forget to include in your notebook file a copy-paste of the commands you used to launch your job along with their outputs.</li>

</ol>