course-info
===========

GitHub Repo for http://db.csail.mit.edu/6.830/


We will be using git, a source code control tool, for labs in 6.830.  This
will allow you to download the code for the labs, and also submit the
labs in a standarized format that will streamline grading.

You will also be able to use git to commit your progress on the labs as you go.

Course git repositories will be hosted as a  repository in GitHub.  GitHub is a website that
hosts runs git servers for thousands of open source projects.  In our case, your code will be
in a private repository that is visible only to you and course staff.

This document describes what you need to do to get started with git, and also download and upload 6.830/6.814 labs via GitHub.

## Contents

- [Learning Git](#learning-git)
- [Setting up GitHub](#setting-up-github)
- [Installing Git](#installing-git)
- [Setting up Git](#setting-up-git)
- [Getting Newly Released Labs](#getting-newly-released-lab)
- [Submitting Your Labs](#submitting-your-lab)
- [Word of Caution](#word-of-caution)
- [Help!](#help)


## Learning Git

There are numerous guides on using Git that are available. They range from being
interactive to just text-based. Find one that works and experiment; making
mistakes and fixing them is a great way to learn. Here is a link to resources
that GitHub suggests:
[https://help.github.com/articles/what-are-other-good-resources-for-learning-git-and-github][resources].

If you have no experience with git, you may find the following web-based tutorial helpful:
[Try Git](https://try.github.io/levels/1/challenges/1).

## <a name="setting-up-github"></a> Setting Up GitHub

Now that you have a basic understanding of Git, it's time to get started
with GitHub.

0. Install git. (See below for suggestions).

1. If you don't already have an account, sign up for one here:
   [https://github.com/join][join].

2. Make sure you are part of the GitHub Organization that we've created for the
   course: MIT-DB-Class. You should have received an invitation to join via email.

   You should now have a repository set up just for your lab solutions.
   This should be called `homework-solns-2015-<athena username>` and located in the MIT-DB-Class organization.

   This is what you'll set up in the next section to allow you to write your
   lab answers and submit them.

### Installing git <a name="installing-git"></a>
The instructions are tested on bash/linux environments. Installing git should be a simple
apt-get / yum / etc install.  

Instructions for installing git on Linux, OSX, or Windows can be found at
[GitBook: Installing](http://git-scm.com/book/en/Getting-Started-Installing-Git).

If you are using Eclipse, many versions come with git configured. The instructions will
be slightly different than the command line instructions listed but will work for any OS. 
Detailed instructions can
be found at [EGit User Guide](http://wiki.eclipse.org/EGit/User_Guide) or 
[EGit Tutorial](http://eclipsesource.com/blogs/tutorials/egit-tutorial).




## Setting Up Git <a name="setting-up-git"></a>

You should have Git installed and have joined the MIT-DB-Class organization from
the previous section.

1. The first thing we have to do is to clone the current lab repository by
   issuing the following commands on the command line:

   ```bash
    $ git clone git@github.com:MIT-DB-Class/simple-db-hw.git
   ```

   This will make a complete replica of the lab repository locally. Now we
   are going to change it to point to your personal repository that was created
   for you in the previous section.

   If you get an error that looks like:

   ```bash
    Cloning into 'lab'...
    Permission denied (publickey).
    fatal: Could not read from remote repository.
    ```

    Most likely the cause is that you just haven't finished setting up your
    GitHub account. You just need to [setup an SSH key][ssh-key] to allow
    pushing and pulling over SSH.

   Change your working path to your newly cloned repository:

   ```bash
    $ cd simple-db-hw/
   ```

2. By default the remote called `origin` is set to the location that you cloned
   the repository from. You should see the following:

   ```bash
    $ git remote -v
        origin git@github.com:MIT-DB-Class/simple-db-hw.git (fetch)
        origin git@github.com:MIT-DB-Class/simple-db-hw.git (push)
   ```

   We don't want that remote to be the origin. Instead, we want to change it to
   point to your repository. To do that, issue the following command:

   ```bash
    $ git remote rename origin upstream
   ```

   And now you should see the following:

   ```bash
    $ git remote -v
        upstream git@github.com:MIT-DB-Class/simple-db-hw.git (fetch)
        upstream git@github.com:MIT-DB-Class/simple-db-hw.git (push)
   ```

3. Lastly we need to give your repository a new `origin` since it is lacking
   one. Issue the following command, substituting your athena username:

   ```bash
    $ git remote add origin git@github.com:MIT-DB-Class/homework-solns-2015-<athena-username>.git
   ```

   If you have an error that looks like the following:

   ```
  Could not rename config section 'remote.[old name]' to 'remote.[new name]'
   ```

   Or this error:
   
   ```
   fatal: remote origin already exists.
   ```
   
   This appears to happen to some depending on the version of Git they are using. To fix it,
   just issue the following command:
   
   ```bash
   $ git remote set-url origin git@github.com:MIT-DB-Class/homework-solns-2015-<athena username>.git
   ```

   This solution was found from [StackOverflow](http://stackoverflow.com/a/2432799) thanks to
   [Cassidy Williams](https://github.com/cassidoo).

   For reference, your final `git remote -v` should look like following when it's
   setup correctly:


   ```bash
    $ git remote -v
        upstream git@github.com:MIT-DB-Class/simple-db-hw.git (fetch)
        upstream git@github.com:MIT-DB-Class/simple-db-hw.git (push)
        origin git@github.com:MIT-DB-Class/homework-solns-2015-<athena username>.git (fetch)
        origin git@github.com:MIT-DB-Class/homework-solns-2015-<athena username>.git (push)
   ```

4. Let's test it out by doing a push of your master branch to GitHub by issuing
   the following:

   ```bash
    $ git push -u origin master
   ```

   You should see something like the following:

   ```
    Counting objects: 5, done.
    Delta compression using up to 4 threads.
    Compressing objects: 100% (3/3), done.
    Writing objects: 100% (3/3), 294 bytes | 0 bytes/s, done.
    Total 3 (delta 2), reused 0 (delta 0)
    To git@github.com:MIT-DB-Class/homework-solns-2015-joshuad.git   f726472..545a4f0  master -> master
   ```

5. That last command was a bit special and only needs to be run the first time
   to setup the remote tracking branches. Now we should be able to just run `git
   push` without the arguments. Try it and you should get the following:

   ```bash
    $ git push
          Everything up-to-date
   ```

If you don't know Git that well, this probably seemed very arcane. Just keep
using Git and you'll understand more and more.   You aren't required to use commands like commit and push as you develop your labs, but will find them useful for debugging.  We'll provide explicit instructions on how to use these commands to actually upload your final lab solution.

## Getting Newly Released Labs <a name="getting-newly-released-lab"></a>

(You don't need to follow these instructions until Lab 2.)

Pulling in labs that are released or previous lab solutions should be
easy as long as you set up your repository based on the instructions in
the last section.

1. All new lab and previous lab solutions will be posted to the
   [labs](https://github.com/MIT-DB-Class/simple-db-hw) repository in the class
   organization.

   Check it periodically as well as Piazza's announcements for updates on
   when the new labs are released.

2. Once a lab is released, create a branch to pull in the changes.  From your simpledb directory:
   ```bash
        $ git checkout -b lab2
   ```
   
  Pulling in the changes should be fairly simple:

   ```bash
        $ git pull upstream master
   ```

   **OR** if you wish to be more explicit, you can `fetch` first and then
   `merge`:

   ```bash
        $ git fetch upstream
        $ git merge upstream/master
   ```
   Now commit to your new branch:
   ```bash
	$ git push origin lab2
   ```

3. If you've followed the instructions in each lab, you should have no
   merge conflicts and everything should be peachy.

## <a name="submitting-your-lab"></a> Submitting Your Labs

You may submit your code multiple times; we will use the latest version you
submit that arrives before the deadline (before 11:59 PM on the due date). 
Place the write-up in a file called <tt>answers.txt</tt> or 
<tt>answers.pdf</tt> in the top level of your <tt>simple-db-hw</tt> directory.  **Important:** 
In order for your write-up to be added to the git repo, you need to explicitly add it:

```bash
$ git add answers.txt
```
You also need to explicitly add any other files you create, such as new *.java 
files.

The criteria for your lab being submitted on time is that your code must be
**tagged** and 
**pushed** by the date and time. This means that if one of the TAs or the
instructor were to open up GitHub, they would be able to see your solutions on
the GitHub web page.

Just because your code has been commited on your local machine does not
mean that it has been **submitted**; it needs to be on GitHub.

There is a bash script `turnInLab1.sh` in the root level directory of simple-db-hw that commits 
your changes, deletes any prior tag
for the current lab, tags the current commit, and pushes the branch and tag 
to github.  If you are using Linux or Mac OSX, you should be able to run the following:

   ```bash
   $ ./turnInLab1.sh
   ```
You should see something like the following output:

 ```bash
 $ ./turnInLab1.sh 
[master b155ba0] Lab 1
 1 file changed, 1 insertion(+)
Deleted tag 'lab1' (was b26abd0)
To git@github.com:MIT-DB-Class/homework-solns-2015-becca.git
 - [deleted]         lab1
Counting objects: 11, done.
Delta compression using up to 4 threads.
Compressing objects: 100% (4/4), done.
Writing objects: 100% (6/6), 448 bytes | 0 bytes/s, done.
Total 6 (delta 3), reused 0 (delta 0)
To git@github.com:MIT-DB-Class/homework-solns-2015-becca.git
   ae31bce..b155ba0  master -> master
Counting objects: 1, done.
Writing objects: 100% (1/1), 152 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@github.com:MIT-DB-Class/homework-solns-2015-becca.git
 * [new tag]         lab1 -> lab1
```


If the above command worked for you, you can skip to item 6 below.  If not, submit your solutions for lab 1 as follows (*replace lab 1 with the correct lab ID for later labs*):

1. Look at your current repository status.

   ```bash
   $ git status
   ```

2. Add and commit your code changes (if they aren't already added and commited).

   ```bash
    $ git commit -a -m 'Lab 1'
   ```

3. Delete any prior local and remote tag (*this will return an error if you have not tagged previously; this allows you to submit multiple times*)

   ```bash
   $ git tag -d lab1
   $ git push origin :refs/tags/lab1
   ```

4. Tag your last commit as the lab to be graded (*again, update the lab ID for later labs*)
   ```bash
   $ git tag -a lab1 -m 'lab1'
   ```

5. This is the most important part: **push** your solutions to GitHub.

   ```bash
   $ git push origin master
   $ git push origin lab1 
   ```

6. The last thing that we strongly recommend you do is to go to the
   [MIT-DB-Class] organization page on GitHub to
   make sure that we can see your solutions.

   Just navigate to your repository and check that your latest commits are on
   GitHub. You should also be able to check 
   `https://github.com/MIT-DB-Class/homework-solns-2015-<athena username>/tree/lab1`


## <a name="word-of-caution"></a> Word of Caution

Git is a distributed version control system. This means everything operates
offline until you run `git pull` or `git push`. This is a great feature.

The bad thing is that you may forget to `git push` your changes. This is why we
strongly, **strongly** suggest that you check GitHub to be sure that what you
want us to see matches up with what you expect.

## <a name="help"></a> Help!

If at any point you need help with setting all this up, feel free to reach out
to one of the TAs or the instructor. Their contact information can be found in the
[syllabus][syllabus].

[join]: https://github.com/join
[resources]: https://help.github.com/articles/what-are-other-good-resources-for-learning-git-and-github
[ssh-key]: https://help.github.com/articles/generating-ssh-keys


This page and infrastructure for this page was taken from https://github.com/MIT-DB-Class/course-info/blob/master/guides/course-setup.md
and https://github.com/jdavis/github-plus-university
