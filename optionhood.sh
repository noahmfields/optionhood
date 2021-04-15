#!/bin/bash

session=8a1822c8-10f3-4012-b368-1ea91cbc60fb
window=3771529b-ebd6-4276-a0ee-7496bd1071f8

tmux kill-session -t $session
tmux new-session -d -s $session
tmux split-window -t $session -v
tmux split-window -t $session -v
tmux rename-window -t $session $window
tmux send-keys -t $session:$window.0 "python3 panes.py positions" C-m
tmux send-keys -t $session:$window.1 "python3 panes.py orders" C-m
tmux send-keys -t $session:$window.2 "python3 commands.py" C-m
tmux send-keys -t $session:$window.2 C-h C-m
tmux attach -d -t $session