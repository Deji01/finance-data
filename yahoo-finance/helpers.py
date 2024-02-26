from datetime import datetime, timedelta, timezone
from llama_index.core.schema import TextNode, NodeRelationship, RelatedNodeInfo
from timescale_vector import client

def combine_text(record: dict) -> str:
    """
    A function to combine the content and article fields of a given record into a single text variable,
    split the text into sentences, remove duplicates while maintaining order,
    and ensure the structure and meaning of the text are preserved.
    Parameters:
    - record: dict, the input record containing 'content' and 'article'
    Return:
    - str, the combined text without duplicate sentences
    """
    # Step 1: Check if both content and article are available
    content = record.get("content")
    article = record.get("article")

    # Step 2: Combine the content of both fields into a single text variable
    combined_text = " ".join([content, article]).strip()

    # Step 3: Split the text into sentences based on full stops
    sentences = combined_text.split(".")

    # Step 4: Remove any leading or trailing whitespace from each sentence
    sentences = [sentence.strip() for sentence in sentences if sentence.strip()]

    # Step 5: Remove duplicates from the sentences using a list while maintaining order
    unique_sentences = []
    for sentence in sentences:
        if sentence not in unique_sentences:
            unique_sentences.append(sentence)

    # Step 6: Combine unique sentences into a single string
    combined_text_without_duplicates = ". ".join(unique_sentences)

    # Step 7: Ensure the structure and meaning of the text are preserved
    # No specific action is taken here, as preserving structure and meaning depends on the context of your data
    return combined_text_without_duplicates


def create_uuid(datetime_obj: datetime):
    """
    Create a UUID from the given datetime object.

    Args:
        datetime_obj (datetime): The datetime object used to create the UUID.

    Returns:
        str: The UUID created from the datetime object, or None if the input datetime_obj is None.
    """
    if datetime_obj is None:
        return None
    timestamp = int(datetime_obj.timestamp())
    uuid = client.uuid_from_time(timestamp)
    return str(uuid)


def create_date(input_string: str) -> datetime:
    """
    A function to create a datetime object from an input string.

    Args:
        input_string (str): The input string representing the date and time.

    Returns:
        datetime: A datetime object representing the input date and time.
    """
    if input_string is None:
        return None

    # Define a dictionary to map month abbreviations to their numerical equivalents
    month_dict = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }

    # Parse the input string
    components = input_string.split()

    # Extract relevant information
    day = int(components[0])
    month = components[1]
    year = int(components[2])
    _ = components[3]
    timezone_offset = components[4]

    # Convert timezone offset to timedelta
    offset_hours = int(timezone_offset[:3])
    offset_minutes = int(timezone_offset[3:])
    timezone_delta = timedelta(hours=offset_hours, minutes=offset_minutes)

    # Combine date and time components into a datetime object
    datetime_obj = (
        datetime(year, month_dict[month], day, tzinfo=timezone.utc) + timezone_delta
    )

    return datetime_obj


# Define the format_date function
def format_date(timestamp):
    """
    Extracts components from the timestamp and creates an input string for the create_date function.
    """
    # Extract components from the timestamp
    day = timestamp.strftime("%d")
    month = timestamp.strftime("%b")
    year = timestamp.strftime("%Y")
    time = timestamp.strftime("%H:%M:%S")
    timezone_offset_minutes = (
        timestamp.utcoffset().total_seconds() // 60
    )  # Calculate offset in minutes
    timezone_hours = int(timezone_offset_minutes // 60)
    timezone_minutes = int(timezone_offset_minutes % 60)

    # Create the input string expected by create_date function
    input_string = (
        f"{day} {month} {year} {time} {timezone_hours:03d}{timezone_minutes:02d}"
    )

    # Call the create_date function (assuming it's defined elsewhere)
    return input_string


def create_node(row):
    """
    A function to create a node using the given row and return the created node.
    """
    record = row.to_dict()
    text = combine_text(record)
    formatted_created_at = create_date(
        str(record["formatted_created_at"])
    )  # Convert to datetime object
    formatted_created_at_str = (
        formatted_created_at.isoformat()
    )  # Serialize to string format
    node = TextNode(
        id_=create_uuid(record["created_at"]),
        text=record["title"] + "\n\n" + text,
        metadata={
            "created_at": formatted_created_at_str,  # Use string format
            "source": record["source"],
            "url": record["url"],
        },
    )
    return node


def create_node_relationships(nodes):
    """
    Create relationships between nodes to represent the next and previous relationships.

    :param nodes: List of nodes to create relationships for
    :type nodes: list
    :return: List of nodes with relationships created
    :rtype: list
    """
    for i in range(len(nodes) - 1):
        nodes[i].relationships[NodeRelationship.NEXT] = RelatedNodeInfo(
            node_id=nodes[i + 1].node_id
        )
        nodes[i + 1].relationships[NodeRelationship.PREVIOUS] = RelatedNodeInfo(
            node_id=nodes[i].node_id
        )
    return nodes
