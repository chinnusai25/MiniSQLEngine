if [[ "$#" -ne 1 ]]; then
	echo "Incorrect Number of Parameters Entered."
	exit 1
fi
python Engine.py "$1"
