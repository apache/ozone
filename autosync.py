#!/opt/homebrew/bin/python3
import subprocess

command = "git log --reverse --no-merges --pretty=format:%H dbd393912f..f7610c0012cd5e83769ff22eebeba092d3893d14"
result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
# Split the output into lines
commits = result.stdout.splitlines()



for commit in commits:
	command = ["git", "cherry-pick", "-x", commit]
	print(commit)
	result = subprocess.run(command, capture_output=True)
	if result.returncode != 0:
	  if result.stderr.decode("utf-8").find("The previous cherry-pick is now empty") != -1:
	    print("the commit is empty, skip it")
	    subprocess.run(["git", "cherry-pick", "--skip"])
	  else:
	    print("Cherry-pick failed. Error:")
	    print(result.stderr.decode("utf-8"))
	    subprocess.run(["git", "status"])
	    answer = input("Do you want to try to fix conflicts and continue (y/n/s)? ")
	    if answer.lower() == "y":
	      # Open an editor for user to fix conflicts
	      subprocess.run(["git", "editconflicts"])
	    elif answer.lower() == 's':
	      subprocess.run(["git", "cherry-pick", "--skip"])
	else:
	  print(f"Successfully cherry-picked commit {commit}")
