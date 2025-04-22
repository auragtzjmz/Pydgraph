import datetime
import json
import pydgraph

def set_schema(client):
    schema = """
    type Student {
        username
        email
        enrollment_date
        location
        enrolled_in
        submits
        follows
    }

    type Instructor {
        name
        expertise
        rating
        teaches
        coauthor
    }

    type Course {
        title
        category
        difficulty_level
        assigned_in
    }

    type Assignment {
        title
        due_date
        score
    }

    username: string @index(exact) .
    email: string .
    enrollment_date: datetime .
    location: geo .

    title: string @index(term) .
    category: string .
    difficulty_level: string .

    name: string @index(exact) .
    expertise: string .
    rating: float @index(float) .

    due_date: datetime .
    score: int .

    enrolled_in: uid @reverse .
    submits: uid @reverse .
    follows: uid @reverse .
    teaches: uid @reverse .
    assigned_in: uid @reverse .
    coauthor: [uid] .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def create_data(client):
    txn = client.txn()
    try:
        data = {
            "uid": "_:melina",
            "dgraph.type": "Student",
            "username": "melina",
            "email": "melina@example.com",
            "enrollment_date": "2024-01-10T00:00:00Z",
            "location": {
                "type": "Point",
                "coordinates": [-103.4167, 20.6667]
            },
            "enrolled_in": {
                "uid": "_:graph_course",
                "dgraph.type": "Course",
                "title": "Graph Databases 101",
                "category": "Databases",
                "difficulty_level": "Beginner",
                "assigned_in": {
                    "uid": "_:assignment1",
                    "dgraph.type": "Assignment",
                    "title": "Intro to Dgraph",
                    "due_date": "2024-02-10T00:00:00Z",
                    "score": 100
                },
                "teaches": {
                    "uid": "_:dr_smith",
                    "dgraph.type": "Instructor",
                    "name": "Dr. Smith",
                    "expertise": "Databases",
                    "rating": 4.8,
                    "coauthor": [
                        {
                            "uid": "_:prof_jane",
                            "dgraph.type": "Instructor",
                            "name": "Prof. Jane",
                            "expertise": "Python",
                            "rating": 4.6
                        }
                    ]
                }
            },
            "submits": {
                "uid": "_:assignment1"
            },
            "follows": {
                "uid": "_:dr_smith"
            }
        }

        response = txn.mutate(set_obj=data)
        txn.commit()
        print(f"Data loaded. UIDs: {response.uids}")
    finally:
        txn.discard()

def query_courses_by_title(client):
    query = """
    query search_courses($title: string) {
        courses(func: anyofterms(title, $title)) {
            uid
            title
            category
        }
    }
    """
    variables = {"$title": "Graph"}
    res = client.txn(read_only=True).query(query, variables=variables)
    print(json.dumps(json.loads(res.json), indent=2))

def query_instructors_by_rating(client):
    query = """
    {
        instructors(func: ge(rating, 4.5)) {
            name
            rating
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    print(json.dumps(json.loads(res.json), indent=2))

def query_students_with_courses(client):
    query = """
    {
        students(func: type(Student)) {
            username
            enrolled_in {
                title
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    print(json.dumps(json.loads(res.json), indent=2))

def query_followers(client):
    query = """
    {
        instructors(func: type(Instructor)) {
            name
            ~follows {
                username
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    print(json.dumps(json.loads(res.json), indent=2))

def delete_low_rating_instructors(client):
    txn = client.txn()
    try:
        query = """
        {
            to_delete(func: lt(rating, 4.0)) {
                uid
            }
        }
        """
        res = txn.query(query)
        data = json.loads(res.json)
        for inst in data["to_delete"]:
            txn.mutate(del_obj=inst)
        txn.commit()
        print("Instructors with rating < 4.0 deleted")
    finally:
        txn.discard()

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
