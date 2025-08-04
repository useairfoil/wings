# Wings Improvement Proposals

This folder contains the Wings Improvement Proposals (WIPs).

## When to create a WIP

Create a WIP when you need to propose a solution to a problem and get feedback from a core contributor.

## Creating a WIP

Create a WIP by copying the `XXXX-TEMPLATE.md` file to a new file in the `wip` folder.

 - bump the version to the next available number (e.g. `0001` -> `0002`),
 - give the WIP a short slug,
 - fill out the metadata.
 
 All WIPs track the following three pieces of metadata:
 
 - authors: the authors of the WIP. Include their name and github profile,
 - status: one of the statuses discussed below,
 - discussion: a link to the pull request to integrate the WIP in the repository.

The WIP goes through the following status during its lifetime:

 - draft: this is the initial state of the WIP. A WIP in this stage should not have all the details ironed out, but it provides a concise summary.
 - discussion: once you're ready to receive more detailed feedback about the WIP, you can change the status to `discussion`. At this stage we will comment on it in more detail and provide active feedback.
 - published: once the WIP has been accepted and it's ready to merge.
 - committed: after the content of the WIP is implemented, its status should be changed to this.
 - abandoned: if, for any reason, the WIP cannot be implemented its status should be changed to this.
