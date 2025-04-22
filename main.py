import os
import pydgraph
import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    mm_options = {
        1: "Load data",
        2: "Query by course title (text index)",
        3: "Query by instructor rating (numeric index)",
        4: "Show students and their courses",
        5: "Show instructor followers (reverse)",
        6: "Delete instructors with low rating",
        7: "Drop all",
        8: "Exit",
    }
    for key in mm_options:
        print(f"{key} -- {mm_options[key]}")

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)

    model.set_schema(client)

    while True:
        print_menu()
        try:
            option = int(input('Enter your choice: '))
        except ValueError:
            print("Invalid input. Please enter a number.")
            continue

        if option == 1:
            model.create_data(client)
        elif option == 2:
            model.query_courses_by_title(client)
        elif option == 3:
            model.query_instructors_by_rating(client)
        elif option == 4:
            model.query_students_with_courses(client)
        elif option == 5:
            model.query_followers(client)
        elif option == 6:
            model.delete_low_rating_instructors(client)
        elif option == 7:
            model.drop_all(client)
        elif option == 8:
            print("Exiting...")
            close_client_stub(client_stub)
            break
        else:
            print("Invalid choice. Please try again.")

if _name_ == '_main_':
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")


'''
Conclusion
This lab was initially a bit challenging to approach due to the 
complexity of modeling multiple interconnected entities. 
However, once the graph model was clearly defined and structured, the 
implementation became much easier. Designing the 
relationships visually and logically made it easier to translate 
the model into Dgraph schema and data mutations.
Overall, it was a great learning experience.
'''

