# Finance talk

## Process

First fetch data from government open data platform

We stumble upon the first problem, there's no documentation. They mention CKAN and that you should use the `ckanapi` library to fetch data from there. Though they link to an outdated version of the library documentation, that is also a dead link.

I figure out how to use the library by checking the latest `ckanapi` docs but every call fails. In the end I had to look into the code to understand what was going on and what I had to change.

ChatGPT obviously wasn't much helpful trying to understand the issue. Though it help me to understand and know which endpoints I had to call to find the information I wanted.

In the end I managed to create a small script that fetches all the info I might need.
