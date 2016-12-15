module Config

//public int TRESHOLD_MIN_SUBTREE_LENGTH = 10; // The minimum length (amount of nodes connected) of a sub-tree in order to detect it as a duplicate. (Described as MassTreshold in the paper)
public int TRESHOLD_MIN_SEQUENCE_LENGTH = 2; // The minimum sequence length of a block in order to detect duplicates in it

public loc RESULT_JSON_FILE = |project://CloneDetection/src/result/flare.json|;