# This file to be sourced in the terminal for development.

function workspace-install {
  poetry shell
  poetry install
}

function workspace-welcome {
  echo "Welcome to the Schematic repository! 👋"
}

function workspace-initialize-env {
  ws-welcome

  export PATH="/home/vscode/.local/bin:$PATH"
}