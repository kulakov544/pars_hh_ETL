big_cities_ids = [1, 2, 3, 4, 1202, 3, 66, 88, 104, 78, 99, 76, 68, 54, 26, 72, 24, 53]
text_search = ['python', 'программист']

search_params_list = []

# Length of the text_search list
text_search_length = len(text_search)

# Generate search parameter sets
for i, city_id in enumerate(big_cities_ids):
    search_text = text_search[i % text_search_length]
    search_params_list.append(
        {"text": search_text, "area": city_id, "per_page": 100, "page": 0}
    )

for i in search_params_list:
    print(search_params_list, '\n')