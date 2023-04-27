"""
main module of basic Mondrian
"""

# !/usr/bin/env python
# coding=utf-8


import pdb
import time

from typing import Tuple

from models.numrange import NumRange
from models.partition import Partition


__DEBUG = False
QI_LEN = 10
GL_K = 0
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []


def get_normalized_width(partition: Partition, qid_index: int) -> float:    
    """
    Return Normalized width of partition. Similar to NCP        

        Parameters
        ----------
        qid_index : int
            The index of the QID in the data
    """        

    if IS_CAT[qid_index] is False:
        low = partition.attribute_width_list[qid_index][0]
        high = partition.attribute_width_list[qid_index][1]
        width = float(ATT_TREES[qid_index].sort_value[high]) - float(ATT_TREES[qid_index].sort_value[low])
    else:
        width = partition.attribute_width_list[qid_index]

    return width * 1.0 / QI_RANGE[qid_index]


def choose_dimension(partition: Partition) -> int:
    """ Chooss QID with largest normlized Width and return its index. """

    max_norm_width = -1
    qid_index = -1

    for i in range(QI_LEN):
        if partition.attribute_split_allowed_list[i] == 0:
            continue
        
        normalized_width = get_normalized_width(partition, i)
        if normalized_width > max_norm_width:
            max_norm_width = normalized_width
            qid_index = i

    if max_norm_width > 1:
        print("Error: max_norm_width > 1")
        pdb.set_trace()
    if qid_index == -1:
        print("cannot find the max dim")
        pdb.set_trace()

    return qid_index


def get_frequency_set(partition: Partition, qid_index: int) -> dict[str, int]:
    """ Count the number of unique values in the dataset for the attribute with the specified index, and thus generate a frequency set    
    
    Returns
    -------
    dict
        the keys are unique string values of the attribute, while the values are the count per unique key
    """

    frequency_set = {}
    for record in partition.members:
        try:
            frequency_set[record[qid_index]] += 1
        except KeyError:
            frequency_set[record[qid_index]] = 1
    return frequency_set


def get_median(partition: Partition, qid_index: int) -> Tuple[str, str, str, str]:
    """ Find the middle of the partition

    Returns
    -------
    (str, str, str, str)
        unique_value_to_split_at: the median
        next_unique_value: the unique value right after the median
        unique_values[0]
        unique_values[-1]
    """

    frequency_set = get_frequency_set(partition, qid_index)    
    # Sort the unique values for the attribute with the specified index
    unique_values = list(frequency_set.keys())
    unique_values.sort(key=lambda x: int(x))
    
    # The number of records in the partition
    num_of_records = sum(frequency_set.values())
    middle_index_of_the_records = num_of_records / 2

    # If there are less then 2k values OR only one (or less) unique value, ...
    if middle_index_of_the_records < GL_K or len(unique_values) <= 1:
        return ('', '', unique_values[0], unique_values[-1])
    
    records_processed = 0
    unique_value_to_split_at = ''
    unique_value_to_split_at_index = 0

    for i, unique_value in enumerate(unique_values):
        # Accumulate The number of records of the partition with the already processed unique values
        records_processed += frequency_set[unique_value]
        # If the number of records processed is more than half of the total amount of records in the partition, we have found the median
        if records_processed >= middle_index_of_the_records:
            unique_value_to_split_at = unique_value
            unique_value_to_split_at_index = i
            break
    # The else keyword in a for loop specifies a block of code to be executed when the loop is finished
    else:
        print("Error: cannot find unique_value_to_split_at")    
    try:
        next_unique_value = unique_values[unique_value_to_split_at_index + 1]
    # If the unique value along which we are splitting is the last one in the list
    except IndexError:
        next_unique_value = unique_value_to_split_at

    return (unique_value_to_split_at, next_unique_value, unique_values[0], unique_values[-1])


def split_numerical_value(numeric_value: str, value_to_split_at: int) -> Tuple[str, str] | str:
    """ Split numeric value along value_to_split_at and return sub ranges """

    range_min_and_max = numeric_value.split(',')
    # If this is not a range ('20,30') any more, but a concrete number (20), simply return the number
    if len(range_min_and_max) <= 1:
        return range_min_and_max[0], range_min_and_max[0]    
    else:
        min = range_min_and_max[0]
        max = range_min_and_max[1]
        # Create two new partitions using the [mix, value_to_split_at] and [value_to_split_at, max] new ranges
        if min == value_to_split_at:
            l_range = min
        else:
            l_range = min + ',' + value_to_split_at
        if max == value_to_split_at:
            r_range = max
        else:
            r_range = value_to_split_at + ',' + max
        return l_range, r_range


def split_numerical_attribute(partition: Partition, qid_index: int) -> list[Partition]:
    """ Split numeric attribute by along the median, creating two new sub-partitions """

    sub_partitions: List[Partition] = []

    (unique_value_to_split_at, next_unique_value, min_unique_value, max_unique_value) = get_median(partition, qid_index)

    p_low = ATT_TREES[qid_index].dict[min_unique_value]
    p_high = ATT_TREES[qid_index].dict[max_unique_value]

    # update middle
    if min_unique_value == max_unique_value:
        partition.attribute_generalization_list[qid_index] = min_unique_value
    else:
        partition.attribute_generalization_list[qid_index] = min_unique_value + ',' + max_unique_value

    partition.attribute_width_list[qid_index] = (p_low, p_high)

    if unique_value_to_split_at == '' or unique_value_to_split_at == next_unique_value:        
        return []
    
    middle_value_index = ATT_TREES[qid_index].dict[unique_value_to_split_at]

    l_attribute_generalization_list = partition.attribute_generalization_list[:]
    r_attribute_generalization_list = partition.attribute_generalization_list[:]
    l_attribute_generalization_list[qid_index], r_attribute_generalization_list[qid_index] = split_numerical_value(partition.attribute_generalization_list[qid_index], unique_value_to_split_at)
    
    l_sub_partition: List[Partition] = []
    r_sub_partition: List[Partition] = []

    for record in partition.members:
        # The index of the attribute value of the record in the numrange.sort_value array
        record_index = ATT_TREES[qid_index].dict[record[qid_index]]

        if record_index <= middle_value_index:
            # l_sub_partition = [min_unique_value, means]
            l_sub_partition.append(record)
        else:
            # r_sub_partition = (mean, max_unique_value]
            r_sub_partition.append(record)

    # The normalized width of all attributes remain the same in the two newly created partitions, except for the one along which we execute the split
    l_attribute_width_list = partition.attribute_width_list[:]
    r_attribute_width_list = partition.attribute_width_list[:]

    # The width of the new, "left" partition is composed of the beginning of the original range and the median value
    l_attribute_width_list[qid_index] = (partition.attribute_width_list[qid_index][0], middle_value_index)
    # The width of the new, "right" partition is composed of the next value after the median value we used and the end of the original range
    r_attribute_width_list[qid_index] = (ATT_TREES[qid_index].dict[next_unique_value], partition.attribute_width_list[qid_index][1])

    sub_partitions.append(Partition(l_sub_partition, l_attribute_width_list, l_attribute_generalization_list, QI_LEN))
    sub_partitions.append(Partition(r_sub_partition, r_attribute_width_list, r_attribute_generalization_list, QI_LEN))

    return sub_partitions

def split_categorical(partition, dim, pattribute_width_list, pattribute_generalization_list):
    """
    split categorical attribute using generalization hierarchy
    """
    sub_partitions = []
    # categoric attributes
    splitVal = ATT_TREES[dim][partition.attribute_generalization_list[dim]]
    sub_node = [t for t in splitVal.child]
    sub_groups = []
    for i in range(len(sub_node)):
        sub_groups.append([])
    if len(sub_groups) == 0:
        # split is not necessary
        return []
    for temp in partition.members:
        qid_value = temp[dim]
        for i, node in enumerate(sub_node):
            try:
                node.cover[qid_value]
                sub_groups[i].append(temp)
                break
            except KeyError:
                continue
        else:
            print("Generalization hierarchy error!")
    flag = True
    for index, sub_group in enumerate(sub_groups):
        if len(sub_group) == 0:
            continue
        if len(sub_group) < GL_K:
            flag = False
            break
    if flag:
        for i, sub_group in enumerate(sub_groups):
            if len(sub_group) == 0:
                continue
            wtemp = pattribute_width_list[:]
            mtemp = pattribute_generalization_list[:]
            wtemp[dim] = len(sub_node[i])
            mtemp[dim] = sub_node[i].value
            sub_partitions.append(Partition(sub_group, wtemp, mtemp, QI_LEN))
    return sub_partitions


def split_partition(partition, dim):
    """
    split partition and distribute records to different sub-partitions
    """
    pattribute_width_list = partition.attribute_width_list
    pattribute_generalization_list = partition.attribute_generalization_list
    if IS_CAT[dim] is False:
        return split_numerical_attribute(partition, dim)
    else:
        return split_categorical(partition, dim, pattribute_width_list, pattribute_generalization_list)


def anonymize(partition):
    """
    Main procedure of Half_Partition.
    recursively partition groups until not allowable.
    """
    # print(len(partition)
    # print(partition.attribute_split_allowed_list
    # pdb.set_trace()
    if check_splitable(partition) is False:
        RESULT.append(partition)
        return
    # Choose dim
    dim = choose_dimension(partition)
    if dim == -1:
        print("Error: dim=-1")
        pdb.set_trace()
    sub_partitions = split_partition(partition, dim)
    if len(sub_partitions) == 0:
        partition.attribute_split_allowed_list[dim] = 0
        anonymize(partition)
    else:
        for sub_p in sub_partitions:
            anonymize(sub_p)


def check_splitable(partition):
    """
    Check if the partition can be further splited while satisfying k-anonymity.
    """
    temp = sum(partition.attribute_split_allowed_list)
    if temp == 0:
        return False
    return True


def init(att_trees, data, k, QI_num=-1):
    """
    reset all global variables
    """
    global GL_K, RESULT, QI_LEN, ATT_TREES, QI_RANGE, IS_CAT
    ATT_TREES = att_trees
    for t in att_trees:
        if isinstance(t, NumRange):
            IS_CAT.append(False)
        else:
            IS_CAT.append(True)
    if QI_num <= 0:
        QI_LEN = len(data[0]) - 1
    else:
        QI_LEN = QI_num
    GL_K = k
    RESULT = []
    QI_RANGE = []


def mondrian(att_trees, data, k, QI_num=-1):
    """
    basic Mondrian for k-anonymity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    init(att_trees, data, k, QI_num)
    result = []
    attribute_generalization_list = []
    wtemp = []
    for i in range(QI_LEN):
        if IS_CAT[i] is False:
            QI_RANGE.append(ATT_TREES[i].range)
            wtemp.append((0, len(ATT_TREES[i].sort_value) - 1))
            attribute_generalization_list.append(ATT_TREES[i].value)
        else:
            QI_RANGE.append(len(ATT_TREES[i]['*']))
            wtemp.append(len(ATT_TREES[i]['*']))
            attribute_generalization_list.append('*')
    whole_partition = Partition(data, wtemp, attribute_generalization_list, QI_LEN)
    start_time = time.time()
    anonymize(whole_partition)
    rtime = float(time.time() - start_time)
    ncp = 0.0
    for partition in RESULT:
        r_ncp = 0.0
        for i in range(QI_LEN):
            r_ncp += get_normalized_width(partition, i)
        temp = partition.attribute_generalization_list
        for i in range(len(partition)):
            result.append(temp + [partition.members[i][-1]])
        r_ncp *= len(partition)
        ncp += r_ncp
    # covert to NCP percentage
    ncp /= QI_LEN
    ncp /= len(data)
    ncp *= 100
    if len(result) != len(data):
        print("Losing records during anonymization!!")
        pdb.set_trace()
    if __DEBUG:
        print("K=%d" % k)
        print("size of partitions")
        print(len(RESULT))
        temp = [len(t) for t in RESULT]
        print(sorted(temp))
        print("NCP = %.2f %%" % ncp)
    return (result, (ncp, rtime))
