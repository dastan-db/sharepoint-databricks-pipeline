## Disclaimers

1.  This does not build production-ready apps.
    -   Neglects security, scaling, code quality
    -   Quality degrades quickly as the project becomes more complex, or the more libraries used
        -   Unfortunate, because LLMs love to write complex code and use lots of libraries
        -   Can mitigate by manually feeding specific context
    -   ... but does that matter for the field? its fantastic at mocking up the "art of the possible"
2.  \_
    Treat it like a video game: save early, save often
    -   As you'll see, cursor takes control of your codebase
    -   Use git. Even if you're not saving to a remote repository! local is nice too
        -   don't worry about committing too often
        -   `git stash` is your friend
        -   not super comfortable with git? cursor can help.... 😈
    -   I'm not going to do this today in the interest of time
3.  \_
    LLMs are really good at html, css, js.
    -   is this surprising? the internet is built on these, and therefore models have the most training data on these
    -   for our purposes, they perform really well when you let them control the html, css, and js
    -   if you must use a python framework, use streamlit because it's the most popular python framework
4.  \_
    Try to make sure it runs locally first **before** deploying
    -   Lots of errors go uncaught today, where databricks may think your app is online when it's not (azure function apps do too)
    -   use a virtual environment and manage it yourself
