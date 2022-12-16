# This file to be sourced in the terminal for development.

function workspace-welcome {
  echo "Welcome to the Schematic repository! 👋"
}

function workspace-initialize-env {
  workspace-welcome

  export PATH="/home/vscode/.local/bin:$PATH"
}