from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Enum, ARRAY, Table
from sqlalchemy.orm import relationship, declarative_base, Session, backref
from sqlalchemy.engine.base import Engine
import enum
import json

Base = declarative_base()

# Enum Types
class ResultModuleCompletionEnum(enum.Enum):
    SUCCESS = 'SUCCESS'
    FAIL_BY_SURRENDER = 'FAIL_BY_SURRENDER'
    FAIL_BY_CALCULATION = 'FAIL_BY_CALCULATION'
    FAIL_BY_REGISTRATION_DEADLINE = 'FAIL_BY_REGISTRATION_DEADLINE'
    FAIL_BY_EXECUTION_DEADLINE = 'FAIL_BY_EXECUTION_DEADLINE'
    FAIL_BY_CHECKING_DEADLINE = 'FAIL_BY_CHECKING_DEADLINE'
    FAIL_BY_RESET_RATING = 'FAIL_BY_RESET_RATING'
    FAIL_BY_TEAM_NOT_FORMED = 'FAIL_BY_TEAM_NOT_FORMED'
    FAIL_BY_ABSENT_DEFAULT_BRANCH = 'FAIL_BY_ABSENT_DEFAULT_BRANCH'
    FAIL_BY_FORGOT_TO_REGISTER_ON_EXAM_EVENT = 'FAIL_BY_FORGOT_TO_REGISTER_ON_EXAM_EVENT'
    FAIL_BY_FORGOT_TO_REGISTER_ON_EXAM_MODULE = 'FAIL_BY_FORGOT_TO_REGISTER_ON_EXAM_MODULE'
    FAIL_BY_FORGOT_TO_REGISTER_ON_BOTH_EXAM_MODULE_AND_EVENT = 'FAIL_BY_FORGOT_TO_REGISTER_ON_BOTH_EXAM_MODULE_AND_EVENT'
    FAIL_BY_STUDENT_ABSENCE_ON_EXAM_EVENT = 'FAIL_BY_STUDENT_ABSENCE_ON_EXAM_EVENT'
    FAIL_BY_EXPELLED_STUDENT = 'FAIL_BY_EXPELLED_STUDENT'
    FAIL_BY_FREEZING_STUDENT = 'FAIL_BY_FREEZING_STUDENT'
    FAIL_BY_SURRENDER_FROM_COURSE = 'FAIL_BY_SURRENDER_FROM_COURSE'
    AIL_BY_SOLUTION_FILE_NOT_FOUND = 'AIL_BY_SOLUTION_FILE_NOT_FOUND'

class TaskCheckEnum (enum.Enum):
    BY_TEACHER = "BY_TEACHER"
    SELF_CHECK = "SELF_CHECK"
    STUDENT_BY_STUDENT = "STUDENT_BY_STUDENT"
    AUTO_CHECK = "AUTO_CHECK"
    WITHOUT_CHECK = "WITHOUT_CHECK"
    CODE_REVIEW = "CODE_REVIEW"
    MENTOR_FEEDBACK = "MENTOR_FEEDBACK"


class GitlabCiCdTypeEnum(enum.Enum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"
    PRIVATE = "PRIVATE"

class TaskSolutionTypeEnum(enum.Enum):
    GITLAB = "GITLAB"
    PLATFORM = "PLATFORM"

class FilledChecklistCheckType(enum.Enum):
    COMMON = "COMMON"
    EXTRA = "EXTRA"
    PRIORITY = "PRIORITY"

class TeamCreateOptionEnum(enum.Enum):
    RANDOM = "RANDOM"
    ALGORITHMIC = "ALGORITHMIC"
    MANUAL = "MANUAL"

class AssignmentType(enum.Enum):
    EXAM = "EXAM"
    INDIVIDUAL = "INDIVIDUAL"
    GROUP = "GROUP"
    EXAM_TEST = "EXAM_TEST"
    INTERNSHIP = "INTERNSHIP"
    COURSE = "COURSE"

# Association Tables
levelTaskAssociation = Table(
    'levelTaskAssociation',
    Base.metadata,
    Column('levelId', Integer, ForeignKey('level.id')),
    Column('taskId', Integer, ForeignKey('task.id'))
)

goalElementTaskAssociation = Table(
    'goalElementTaskAssociation',
    Base.metadata,
    Column('goalElementId', Integer, ForeignKey('goalElement.id')),
    Column('taskId', Integer, ForeignKey('task.id'))
)

# Main Models
class ProjectTimelineItem(Base):
    __tablename__ = 'projectTimelineItem'
    id = Column(Integer, primary_key=True)
    type = Column(String)
    elementType = Column(String)
    status = Column(String)
    start = Column(DateTime)
    end = Column(DateTime)
    order = Column(Integer)
    parentId = Column(Integer, ForeignKey('projectTimelineItem.id'))
    children = relationship(
        'ProjectTimelineItem',
        backref=backref('parent', remote_side=[id]),
        foreign_keys=[parentId]
    )

class ProjectReviewsInfo(Base):
    __tablename__ = 'projectReviewsInfo'
    id = Column(Integer, primary_key=True)
    reviewByStudentCount = Column(Integer)
    relevantReviewByStudentsCount = Column(Integer)
    reviewByInspectionStaffCount = Column(Integer)
    relevantReviewByInspectionStaffCount = Column(Integer)

class StudentTask(Base):
    __tablename__ = 'studentTask'
    id = Column(Integer, primary_key=True)
    taskId = Column(Integer, ForeignKey('task.id'))
    lastAnswerId = Column(Integer, ForeignKey('studentAnswer.id'))
    teamSettingsId = Column(Integer, ForeignKey('teamSettings.id'))
    
    task = relationship('Task', back_populates='studentTasks')
    lastAnswer = relationship('StudentAnswer')
    teamSettings = relationship('TeamSettings')

class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    assignmentType = Column(Enum(AssignmentType))
    taskSolutionType = Column(Enum(TaskSolutionTypeEnum))
    checkTypes = Column(Enum(FilledChecklistCheckType))
    studentTasks = relationship('StudentTask', back_populates='task')
    levels = relationship(
        'Level',
        secondary=levelTaskAssociation,
        back_populates='tasks'
    )
    goalElements = relationship(
        'GoalElement',
        secondary=goalElementTaskAssociation,
        back_populates='tasks'
    )

class Level(Base):
    __tablename__ = 'level'
    id = Column(Integer, primary_key=True)
    studyModuleId = Column(Integer, ForeignKey('studyModule.id'))
    goalElements = relationship('GoalElement', backref='level')
    tasks = relationship(
        'Task',
        secondary=levelTaskAssociation,
        back_populates='levels'
    )

class GoalElement(Base):
    __tablename__ = 'goalElement'
    id = Column(Integer, primary_key=True)
    levelId = Column(Integer, ForeignKey('level.id'))
    tasks = relationship(
        'Task',
        secondary='goalElementTaskAssociation',
        back_populates='goalElements'
    )

class StudentTaskAdditionalAttributes(Base):
    __tablename__ = 'studentTaskAdditionalAttributes'
    id = Column(Integer, primary_key=True)
    taskId = Column(Integer, ForeignKey('task.id'))
    cookiesCount = Column(Integer)
    maxCodeReviewCount = Column(Integer)
    codeReviewCost = Column(Integer)
    ciCdMode = Column(Boolean)
    task = relationship('Task')

class TeamSettings(Base):
    __tablename__ = 'teamSettings'
    id = Column(Integer, primary_key=True)
    teamCreateOption = Column(Enum(TeamCreateOptionEnum))
    minAmountMember = Column(Integer)
    maxAmountMember = Column(Integer)
    enableSurrenderTeam = Column(Boolean)

    def __repr__(self):
        return f"TeamSettings(id={self.id}, teamCreateOption={self.teamCreateOption}, minAmountMember={self.minAmountMember}, maxAmountMember={self.maxAmountMember})"

class ModuleAttemptsSettings(Base):
    __tablename__ = 'moduleAttemptsSettings'
    id = Column(Integer, primary_key=True)
    maxModuleAttempts = Column(Integer)
    isUnlimitedAttempts = Column(Boolean)

    def __repr__(self):
        return f"ModuleAttemptsSettings(id={self.id}, maxModuleAttempts={self.maxModuleAttempts}, isUnlimitedAttempts={self.isUnlimitedAttempts})"

class StudentCodeReviewsWithCountRound(Base):
    __tablename__ = 'studentCodeReviewsWithCountRound'
    id = Column(Integer, primary_key=True)
    countRound1 = Column(Integer)
    countRound2 = Column(Integer)
    codeReviewsInfoId = Column(Integer, ForeignKey('codeReviewsInfo.id'))
    codeReviewsInfo = relationship('CodeReviewsInfo')

    def __repr__(self):
        return f"StudentCodeReviewsWithCountRound(id={self.id}, countRound1={self.countRound1}, countRound2={self.countRound2})"

class CodeReviewsInfo(Base):
    __tablename__ = 'codeReviewsInfo'
    id = Column(Integer, primary_key=True)
    maxCodeReviewCount = Column(Integer, default=0)
    codeReviewDuration = Column(Integer, default=0)
    codeReviewCost = Column(Integer, default=0)

    def __repr__(self):
        return f"CodeReviewsInfo(id={self.id}, maxCodeReviewCount={self.maxCodeReviewCount}, codeReviewDuration={self.codeReviewDuration}, codeReviewCost={self.codeReviewCost})"

class P2PChecksInfo(Base):
    __tablename__ = 'P2PChecksInfo'
    id = Column(Integer, primary_key=True)
    cookiesCount = Column(Integer)
    periodOfVerification = Column(Integer)
    projectReviewsInfoId = Column(Integer, ForeignKey('projectReviewsInfo.id'))
    projectReviewsInfo = relationship('ProjectReviewsInfo')

    def __repr__(self):
        return f"P2PChecksInfo(id={self.id}, cookiesCount={self.cookiesCount}, periodOfVerification={self.periodOfVerification})"

class SoftSkill(Base):
    __tablename__ = 'softSkill'
    id = Column(Integer, primary_key=True)
    softSkillId = Column(Integer)
    softSkillName = Column(String)
    totalPower = Column(Integer)
    maxPower = Column(Integer)
    currentUserPower = Column(Integer)
    achievedUserPower = Column(Integer)
    teamRole = Column(String)
    moduleCoverId = Column(Integer, ForeignKey('moduleCoverInformation.id'))

    def __repr__(self):
        return f"SoftSkill(id={self.id}, softSkillId={self.softSkillId}, softSkillName={self.softSkillName})"

class ModuleCoverInformation(Base):
    __tablename__ = 'moduleCoverInformation'
    id = Column(Integer, primary_key=True)
    isOwnStudentTimeline = Column(Boolean)
    softSkills = relationship('SoftSkill', backref='moduleCover')
    timelineId = Column(Integer, ForeignKey('projectTimelineItem.id'))
    timeline = relationship('ProjectTimelineItem')

    def __repr__(self):
        return f"ModuleCoverInformation(id={self.id}, isOwnStudentTimeline={self.isOwnStudentTimeline})"

class StudyModule(Base):
    __tablename__ = 'studyModule'
    id = Column(Integer, primary_key=True)
    idea = Column(String)
    duration = Column(Integer)
    goalPoint = Column(Integer)
    retrySettingsId = Column(Integer, ForeignKey('moduleAttemptsSettings.id'))
    retrySettings = relationship('ModuleAttemptsSettings')
    levels = relationship('Level', backref='studyModule')

    def __repr__(self):
        return f"StudyModule(id={self.id}, idea='{self.idea}', duration={self.duration}, goalPoint={self.goalPoint})"

class StudentModule(Base):
    __tablename__ = 'studentModule'
    id = Column(Integer, primary_key=True)
    moduleTitle = Column(String)
    finalPercentage = Column(Integer)
    finalPoint = Column(Integer)
    goalExecutionType = Column(String)
    displayedGoalStatus = Column(String)
    accessBeforeStartProgress = Column(Boolean)
    resultModuleCompletion = Column(Enum(ResultModuleCompletionEnum))
    finishedExecutionDateByScheduler = Column(DateTime)
    durationFromStageSubjectGroupPlan = Column(Integer)
    currentAttemptNumber = Column(Integer)
    isDeadlineFree = Column(Boolean)
    isRetryAvailable = Column(Boolean)
    localCourseId = Column(Integer)
    studyModuleId = Column(Integer, ForeignKey('studyModule.id'))
    currentTaskId = Column(Integer, ForeignKey('studentTask.id'))
    teamSettingsId = Column(Integer, ForeignKey('teamSettings.id'))
    
    studyModule = relationship('StudyModule')
    currentTask = relationship('StudentTask')
    teamSettings = relationship('TeamSettings')
    courseBaseParameters = relationship('CourseBaseParameters', uselist=False)

    def __repr__(self):
        return f"StudentModule(id={self.id}, moduleTitle='{self.moduleTitle}', finalPercentage={self.finalPercentage}, finalPoint={self.finalPoint})"

class CourseBaseParameters(Base):
    __tablename__ = 'courseBaseParameters'
    id = Column(Integer, primary_key=True)
    studentModuleId = Column(Integer, ForeignKey('studentModule.id'))
    isGradedCourse = Column(Boolean)
    
    def ___repr__(self):
        return f"CourseBaseParameters(id={self.id}, studentModuleId={self.studentModuleId}, isGradedCourse={self.isGradedCourse})"

class StudentAnswer(Base):
    __tablename__ = 'studentAnswer'
    id = Column(Integer, primary_key=True)


# Database Manager Class ----------------------------------------------------
class ProjectDatabase:
    def __init__(self, engine: Engine):
        self.engine = engine
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def save_project_info(self, project_data: dict):
        try:
            student_data = project_data.get('student', {})
            
            # Сохранение StudentModule и связанных данных
            module_data = student_data.get('getModuleById', {})
            if module_data:
                # Сохраняем связанные настройки
                retrySettings_data = module_data.get('studyModule', {}).get('retrySettings', {})
                retrySettings = ModuleAttemptsSettings(**retrySettings_data)
                self.session.add(retrySettings)
                self.session.flush()

                # Сохраняем StudyModule
                study_module_data = module_data.get('studyModule', {})
                study_module = StudyModule(
                    retrySettingsId=retrySettings.id,
                    **{k: v for k, v in study_module_data.items() if k != 'levels' and k != 'retrySettings'}
                )
                self.session.add(study_module)
                self.session.flush()

                # Сохраняем Levels и Tasks
                for level_data in study_module_data.get('levels', []):
                    level = Level(studyModuleId=study_module.id)
                    self.session.add(level)
                    self.session.flush()
                    
                    task_ids = []
                    for task_data in level_data.get('tasks', []):
                        task = Task(**task_data)
                        self.session.add(task)
                        self.session.flush()
                        task_ids.append(task.id)
                    
                    # Добавляем ассоциации
                    stmt = levelTaskAssociation.insert().values(
                        [(level.id, task_id) for task_id in task_ids]
                    )
                    self.session.execute(stmt)

                # Сохраняем StudentModule
                student_module = StudentModule(
                    studyModuleId=study_module.id,
                    **{k: v for k, v in module_data.items() 
                      if k not in ['studyModule', 'currentTask', 'teamSettings', 'courseBaseParameters']}
                )
                self.session.add(student_module)
                self.session.flush()

            # Сохранение ModuleCoverInformation
            cover_data = student_data.get('getModuleCoverInformation', {})
            if cover_data:
                def save_timeline(timeline_data, parent_id=None):
                    timeline = ProjectTimelineItem(
                        parentId=parent_id,
                        **{k: v for k, v in timeline_data.items() if k != 'children' and v is not None}
                    )
                    self.session.add(timeline)
                    self.session.flush()
                    for child in timeline_data.get('children', []) or []:  # Обрабатываем как null, так и пустой список
                        save_timeline(child, timeline.id)
                    return timeline.id

                # Обрабатываем каждый элемент списка timeline
                timeline_data = cover_data.get('timeline', [])
                timeline_ids = [save_timeline(item) for item in timeline_data]
                
                # Сохраняем ModuleCoverInformation
                for timeline_id in timeline_ids:
                    cover = ModuleCoverInformation(
                        timelineId=timeline_id,
                        **{k: v for k, v in cover_data.items() if k not in  ['softSkills', 'timeline']}
                    )

                if not timeline_ids:
                    cover = ModuleCoverInformation(
                        timelineId=None,
                        **{k: v for k, v in cover_data.items() if k not in  ['softSkills', 'timeline']}
                    )
                    self.session.add(cover)
                    self.session.flush()
                
                # Сохраняем SoftSkills
                for skill_data in cover_data.get('softSkills', []):
                    skill = SoftSkill(moduleCoverId=cover.id, **skill_data)
                    self.session.add(skill)

            # Сохранение P2PChecksInfo
            p2p_data = student_data.get('getP2PChecksInfo', {})
            if p2p_data:
                reviews_info = ProjectReviewsInfo(**p2p_data.get('projectReviewsInfo', {}))
                self.session.add(reviews_info)
                self.session.flush()
                
                p2p = P2PChecksInfo(
                    projectReviewsInfoId=reviews_info.id,
                    **{k: v for k, v in p2p_data.items() if k != 'projectReviewsInfo'}
                )
                self.session.add(p2p)

            # Сохранение StudentCodeReviewsWithCountRound
            code_reviews_data = student_data.get('getStudentCodeReviewByGoalId', {})
            if code_reviews_data:
                code_reviews_info_data = code_reviews_data.get('codeReviewsInfo', {})
                code_reviews_info = CodeReviewsInfo(**(code_reviews_info_data or {}))
                self.session.add(code_reviews_info)
                self.session.flush()
                
                code_reviews = StudentCodeReviewsWithCountRound(
                    codeReviewsInfoId=code_reviews_info.id,
                    **{k: v for k, v in code_reviews_data.items()  if k != 'codeReviewsInfo'} 
                )
                self.session.add(code_reviews)
            
            self.session.commit()
            return True
        
        except Exception as e:
            import traceback
            self.session.rollback()
            traceback.print_exc()
            raise RuntimeError(f"Ошибка сохранения данных: {str(e)}")
    
    def cleanup(self):
        Base.metadata.drop_all(self.engine)

    def close(self):
        self.session.close()

# Helper Functions ----------------------------------------------------------
def create_from_json(engine: Engine, json: dict):
    db = ProjectDatabase(engine)
    try:
        db.save_project_info(json)
    finally:
        db.close()