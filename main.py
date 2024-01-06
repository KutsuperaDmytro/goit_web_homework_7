from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result

def select_03():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  = 3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id = g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date)\
        .select_from(Grade)\
        .join(Student)\
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


def select_04():
    """
    SELECT
        s.group_id,
        ROUND(AVG(g.grade), 2) AS average_grade_on_stream
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.group_id
    ORDER BY average_grade_on_stream DESC;
    :return:
    """
    average_grade_all_students = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade_all_students')).scalar()
    average_grade_per_group = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade_on_stream')) \
        .join(Grade).group_by(Student.group_id).order_by(desc('average_grade_on_stream')).all()

    return average_grade_all_students, average_grade_per_group


def select_05(teacher_fullname):
    """
    SELECT subjects.name AS course_name
    FROM subjects
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE teachers.fullname = 'Tiffany Kennedy';
    --WHERE teachers.fullname = 'Sarah Williams';
    --WHERE teachers.fullname = 'Corey David';
    --WHERE teachers.fullname = 'Tonya Smith DDS';
    --WHERE teachers.fullname = 'Brandon Whitehead';
    --WHERE teachers.fullname = 'Justin Wolfe';
    :return:
    """
    result = session.query(Subject.name.label('course_name')) \
        .join(Teacher.disciplines).filter(Teacher.fullname == teacher_fullname).all()
    return result

def select_06(group_name):
    """
    SELECT students.fullname
    FROM students
    JOIN groups ON students.group_id = groups.id
    WHERE groups.name = 'if';
    --WHERE groups.name = 'fish';
    --WHERE groups.name = 'accept';
    --WHERE groups.name = 'top';
    --WHERE groups.name = 'option';
    --WHERE groups.name = 'of';
    :return:
    """
    result = session.query(Student.fullname) \
        .join(Group).filter(Group.name == group_name).all()
    return result

def select_07(subject_id):
    """
    SELECT g.name AS group_name, AVG(gd.grade) AS average_grade
    FROM groups g
    JOIN students s ON g.id = s.group_id
    JOIN grades gd ON s.id = gd.student_id
    WHERE gd.id = subject_id
    GROUP BY g.id
    ORDER BY average_grade DESC;
    """
    result = session.query(Group.name.label('group_name'), func.avg(Grade.grade).label('average_grade')) \
        .select_from(Group) \
        .join(Student).join(Grade).filter(Grade.subjects_id == subject_id) \
        .group_by(Group.id).order_by(desc('average_grade')).all()
    return result


def select_08():
    """
    SELECT t.fullname AS teacher_name, AVG(gd.grade) AS average_grade
    FROM teachers t
    JOIN subjects subj ON t.id = subj.teacher_id
    JOIN grades gd ON subj.id = gd.subject_id
    GROUP BY t.id, t.fullname
    ORDER BY average_grade DESC;
    """
    result = session.query(Teacher.fullname.label('teacher_name'), func.avg(Grade.grade).label('average_grade')) \
        .select_from(Teacher) \
        .join(Subject).join(Grade).group_by(Teacher.id, Teacher.fullname).order_by(desc('average_grade')).all()
    return result

def select_09(student_id):
    """
    SELECT
        students.fullname AS student_name,
        subjects.name AS subject_name,
        teachers.fullname AS teacher_name
    FROM
        students
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE
    students.id = :student_id;
    :return:
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Subject.name.label('subject_name'),
            Teacher.fullname.label('teacher_name')
        )
        .select_from(Student)  # Explicitly set the left side of the join
        .join(Grade)
        .join(Subject)
        .join(Teacher)
        .filter(Student.id == student_id)
        .all()
    )

    return result


def select_10(student_id, teacher_id):
    """
    SELECT
      students.fullname AS student_name,
      subjects.name AS subject_name
    FROM
      students
      JOIN grades ON students.id = grades.student_id
      JOIN subjects ON grades.subject_id = subjects.id
      JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE
      students.id = 13
      AND teachers.id = 1;
      :return:
    """
    result = session.query(
        Student.fullname.label('student_name'),
        Subject.name.label('subject_name')
    ).join(Grade).join(Subject).join(Teacher).filter(
        Student.id == student_id,
        Teacher.id == teacher_id
    ).all()

    return result

if __name__ == '__main__':
    group_name = 'if'
    subject_id = 1
    student_id = 23  # Replace with the actual student ID
    students_in_group = select_06(group_name)
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05('Tiffany Kennedy'))
    print(select_06(group_name))
    print(students_in_group)
    print(select_07(subject_id))
    print(select_08())
    print(select_09(student_id))
    print(select_12())