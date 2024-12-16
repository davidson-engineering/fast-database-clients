import timeit
import random
import string
from itertools import islice


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def cumulative_chunks(lst, target_length):
    """Yield successive chunks from lst based on the cumulative length of 'field' values."""
    chunk = []
    cumulative_length = 0

    append = chunk.append  # Local reference to speed up loop
    for d in lst:
        field_length = len(
            d["field"]
        )  # Access 'field' directly, assuming it's always present
        if cumulative_length + field_length > target_length:
            yield chunk
            chunk = []
            cumulative_length = 0
            append = chunk.append  # Update local reference for new chunk

        append(d)
        cumulative_length += field_length

    if chunk:
        yield chunk


# Generate sample data
def generate_data(num_dicts, max_field_length):
    return [
        {
            "field": "".join(
                random.choices(
                    string.ascii_lowercase, k=random.randint(1, max_field_length)
                )
            )
        }
        for _ in range(num_dicts)
    ]


# Testing parameters
num_dicts = 1000
max_field_length = 10
n = 10
target_length = 50

# Generate the sample data
data = generate_data(num_dicts, max_field_length)

# Test the performance of each function
chunks_time = timeit.timeit(lambda: list(chunks(data, n)), number=1000)
cumulative_chunks_time = timeit.timeit(
    lambda: list(cumulative_chunks(data, target_length)), number=1000
)

print(f"chunks function execution time: {chunks_time:.4f} seconds")
print(
    f"cumulative_chunks function execution time: {cumulative_chunks_time:.4f} seconds"
)
