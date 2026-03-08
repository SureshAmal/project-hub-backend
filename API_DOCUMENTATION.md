# Project Hub - Python FastAPI Backend API Documentation

## Base URL
```
http://localhost:5000/api
```

## Authentication

All protected endpoints require a JWT token in the Authorization header:
```
Authorization: Bearer <access_token>
```

### Token Response
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenType": "bearer"
}
```

---

## Table of Contents
1. [Authentication](#1-authentication)
2. [User](#2-user)
3. [Domains](#3-domains)
4. [Projects](#4-projects)
5. [GitHub Projects](#5-github-projects)
6. [Notifications](#6-notifications)
7. [Analytics](#7-analytics)
8. [Workspace](#8-workspace)
9. [Gamification](#9-gamification)
10. [Learning Paths](#10-learning-paths)

---

## 1. Authentication

### 1.1 Register User
**POST** `/api/auth/register`

Register a new user with email and password.

#### Request Payload
```json
{
  "email": "user@example.com",
  "password": "securePassword123",
  "name": "John Doe",
  "firstName": "John",
  "lastName": "Doe"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| email | string | Yes | Valid email address |
| password | string | Yes | Password (min 6 characters) |
| name | string | No | Full name |
| firstName | string | No | First name |
| lastName | string | No | Last name |

#### Response (201 Created)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe",
  "firstName": "John",
  "lastName": "Doe",
  "image": null,
  "profileImage": null,
  "bio": null,
  "headline": null,
  "location": null,
  "githubUrl": null,
  "portfolioUrl": null,
  "role": "STUDENT",
  "isVerified": false,
  "emailVerified": null,
  "createdAt": "2026-03-02T10:00:00Z",
  "updatedAt": "2026-03-02T10:00:00Z",
  "totalTimeSpent": 0,
  "currentStreak": 0,
  "longestStreak": 0,
  "points": 0,
  "level": 1,
  "lastActiveDate": null
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 400 | Email already registered | Email already exists |

---

### 1.2 Login
**POST** `/api/auth/login`

Authenticate with email and password.

#### Request Payload
```json
{
  "email": "user@example.com",
  "password": "securePassword123"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| email | string | Yes | Registered email |
| password | string | Yes | User password |

#### Response (200 OK)
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenType": "bearer"
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 401 | Incorrect email or password | Invalid credentials |

---

### 1.3 Get Current User
**GET** `/api/auth/me`

Get the currently authenticated user's profile.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe",
  "firstName": "John",
  "lastName": "Doe",
  "image": null,
  "profileImage": "https://...",
  "bio": "Full-stack developer",
  "headline": "Software Engineer",
  "location": "San Francisco, CA",
  "githubUrl": "https://github.com/johndoe",
  "portfolioUrl": "https://johndoe.dev",
  "role": "STUDENT",
  "isVerified": false,
  "emailVerified": null,
  "createdAt": "2026-03-02T10:00:00Z",
  "updatedAt": "2026-03-02T10:00:00Z",
  "totalTimeSpent": 120,
  "currentStreak": 5,
  "longestStreak": 10,
  "points": 1500,
  "level": 3,
  "lastActiveDate": "2026-03-02T08:00:00Z"
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 401 | Could not validate credentials | Invalid or missing token |

---

### 1.4 Refresh Token
**POST** `/api/auth/refresh`

Refresh an expired access token.

#### Request Payload
```json
{
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| refreshToken | string | Yes | Valid refresh token |

#### Response (200 OK)
```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

---

### 1.5 Logout
**POST** `/api/auth/logout`

Logout the current user.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "message": "Logged out successfully"
}
```

---

## 2. User

### 2.1 Get User Progress
**GET** `/api/user/progress`

Get the authenticated user's standard project progress.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "projectId": "660e8400-e29b-41d4-a716-446655440001",
    "status": "IN_PROGRESS",
    "startedAt": "2026-03-01T10:00:00Z",
    "completedAt": null,
    "timeSpent": 45,
    "isRunning": false,
    "lastTimerStart": null,
    "targetCompletionDate": "2026-03-15T00:00:00Z",
    "notes": "Working on the API integration",
    "createdAt": "2026-03-01T10:00:00Z",
    "updatedAt": "2026-03-02T08:00:00Z",
    "project": {
      "id": "660e8400-e29b-41d4-a716-446655440001",
      "title": "E-commerce Platform"
    }
  }
]
```

---

### 2.2 Get GitHub Progress
**GET** `/api/user/github-progress`

Get the authenticated user's GitHub project progress.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "githubProjectId": "660e8400-e29b-41d4-a716-446655440002",
    "status": "IN_PROGRESS",
    "startedAt": "2026-03-01T10:00:00Z",
    "completedAt": null,
    "timeSpent": 3600,
    "notes": "Setting up the project structure",
    "checklist": [true, true, false, false, false],
    "createdAt": "2026-03-01T10:00:00Z",
    "updatedAt": "2026-03-02T08:00:00Z",
    "githubProject": {
      "id": "660e8400-e29b-41d4-a716-446655440002",
      "title": "React Dashboard",
      "repoOwner": "facebook",
      "repoName": "react-dashboard"
    }
  }
]
```

---

### 2.3 Get Single GitHub Progress
**GET** `/api/user/github-progress/{project_id}`

Get progress for a specific GitHub project.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "githubProjectId": "660e8400-e29b-41d4-a716-446655440002",
  "status": "NOT_STARTED",
  "startedAt": null,
  "completedAt": null,
  "timeSpent": 0,
  "notes": null,
  "checklist": [],
  "createdAt": "2026-03-02T10:00:00Z",
  "updatedAt": "2026-03-02T10:00:00Z",
  "githubProject": {
    "id": "660e8400-e29b-41d4-a716-446655440002",
    "title": "React Dashboard",
    "repoOwner": "facebook",
    "repoName": "react-dashboard"
  }
}
```

---

### 2.4 Update GitHub Progress
**PUT** `/api/user/github-progress/{project_id}`

Update progress for a specific GitHub project.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Request Payload
```json
{
  "status": "IN_PROGRESS",
  "timeSpent": 1800,
  "notes": "Completed authentication module",
  "checklist": [true, true, false, false, false]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| status | string | Yes | NOT_STARTED, IN_PROGRESS, COMPLETED |
| timeSpent | integer | No | Time spent in seconds |
| notes | string | No | Personal notes |
| checklist | array | No | Array of booleans for task completion |

#### Response (200 OK)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "githubProjectId": "660e8400-e29b-41d4-a716-446655440002",
  "status": "IN_PROGRESS",
  "startedAt": "2026-03-02T10:00:00Z",
  "completedAt": null,
  "timeSpent": 1800,
  "notes": "Completed authentication module",
  "checklist": [true, true, false, false, false],
  "createdAt": "2026-03-01T10:00:00Z",
  "updatedAt": "2026-03-02T10:00:00Z",
  "githubProject": {
    "id": "660e8400-e29b-41d4-a716-446655440002",
    "title": "React Dashboard",
    "repoOwner": "facebook",
    "repoName": "react-dashboard"
  }
}
```

---

### 2.5 Update Profile
**PUT** `/api/user/profile`

Update user profile information.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Request Payload
```json
{
  "firstName": "John",
  "lastName": "Doe",
  "headline": "Senior Developer",
  "bio": "Passionate about building great products",
  "location": "New York, NY",
  "githubUrl": "https://github.com/johndoe",
  "portfolioUrl": "https://johndoe.dev"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| firstName | string | No | First name |
| lastName | string | No | Last name |
| headline | string | No | Job title |
| bio | string | No | Biography |
| location | string | No | Location |
| githubUrl | string | No | GitHub profile URL |
| portfolioUrl | string | No | Portfolio URL |

#### Response (200 OK)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe",
  "firstName": "John",
  "lastName": "Doe",
  "headline": "Senior Developer",
  "bio": "Passionate about building great products",
  "location": "New York, NY",
  "githubUrl": "https://github.com/johndoe",
  "portfolioUrl": "https://johndoe.dev",
  ...
}
```

---

### 2.6 Upload Profile Image
**POST** `/api/user/profile/image`

Upload a new profile image.

#### Headers
```
Authorization: Bearer <access_token>
Content-Type: multipart/form-data
```

#### Form Data
| Field | Type | Description |
|-------|------|-------------|
| file | file | Image file (jpg, png, etc.) |

#### Response (200 OK)
```json
{
  "message": "Profile image updated successfully",
  "profileImage": "/uploads/profiles/550e8400-e29b-41d4-a716-446655440000.jpg"
}
```

---

### 2.7 Get Profile Stats
**GET** `/api/user/profile-stats`

Get aggregated profile statistics.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "projectsCompleted": 12,
  "bookmarksCount": 8,
  "xp": 1500,
  "contributions": 0
}
```

---

### 2.8 Get User Activity
**GET** `/api/user/activity`

Get recent activity feed.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "message": "Started project React Dashboard",
    "type": "PROJECT_STARTED",
    "relatedProjectId": "660e8400-e29b-41d4-a716-446655440002",
    "isRead": false,
    "createdAt": "2026-03-02T10:00:00Z"
  }
]
```

---

### 2.9 Get Bookmarks
**GET** `/api/user/bookmarks`

Get all bookmarks for the authenticated user.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "project": null,
    "githubProject": {
      "id": "660e8400-e29b-41d4-a716-446655440002",
      "title": "React Dashboard",
      "slug": "facebook-react-dashboard",
      "description": "A modern dashboard built with React",
      "repoUrl": "https://github.com/facebook/react-dashboard",
      "stars": 1250,
      "forks": 340,
      "language": "TypeScript",
      "difficulty": "INTERMEDIATE"
    },
    "createdAt": "2026-03-01T10:00:00Z"
  }
]
```

---

### 2.10 Check Bookmark
**GET** `/api/user/bookmarks/{project_id}/check`

Check if a specific project is bookmarked.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "bookmarked": true
}
```

---

### 2.11 Batch Check Bookmarks
**POST** `/api/user/bookmarks/batch-check`

Check bookmark status for multiple projects.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Request Payload
```json
{
  "projectIds": [
    "660e8400-e29b-41d4-a716-446655440001",
    "660e8400-e29b-41d4-a716-446655440002",
    "660e8400-e29b-41d4-a716-446655440003"
  ]
}
```

#### Response (200 OK)
```json
{
  "bookmarks": {
    "660e8400-e29b-41d4-a716-446655440001": true,
    "660e8400-e29b-41d4-a716-446655440002": false,
    "660e8400-e29b-41d4-a716-446655440003": true
  }
}
```

---

### 2.12 Toggle Bookmark
**POST** `/api/user/bookmarks/{project_id}`

Toggle bookmark status for a project.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Request Payload (Optional)
```json
{
  "type": "github"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | No | "github" | "github" or "standard" |

#### Response (200 OK)
```json
{
  "bookmarked": true
}
```

---

## 3. Domains

### 3.1 Get All Domains
**GET** `/api/domains`

Get a list of all professional domains.

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Web Development",
    "slug": "web-development",
    "description": "Build modern web applications",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  },
  {
    "id": "550e8400-e29b-41d4-a716-446655440001",
    "name": "Artificial Intelligence",
    "slug": "artificial-intelligence",
    "description": "AI and machine learning projects",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  },
  {
    "id": "550e8400-e29b-41d4-a716-446655440002",
    "name": "Machine Learning",
    "slug": "machine-learning",
    "description": "ML algorithms and models",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  },
  {
    "id": "550e8400-e29b-41d4-a716-446655440003",
    "name": "Data Science",
    "slug": "data-science",
    "description": "Data analysis and visualization",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  },
  {
    "id": "550e8400-e29b-41d4-a716-446655440004",
    "name": "Cybersecurity",
    "slug": "cybersecurity",
    "description": "Security and ethical hacking",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  }
]
```

---

### 3.2 Get Domain by Slug
**GET** `/api/domains/slug/{domain_slug}`

Get a single domain by its slug.

#### Response (200 OK)
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Web Development",
  "slug": "web-development",
  "description": "Build modern web applications",
  "createdAt": "2026-01-01T00:00:00Z",
  "updatedAt": "2026-01-01T00:00:00Z"
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 404 | Domain not found | Domain doesn't exist |

---

## 4. Projects

### 4.1 Get All Projects
**GET** `/api/projects`

Get a list of all GitHub projects with optional filtering and pagination.

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| skip | integer | 0 | Number of records to skip |
| limit | integer | 50 | Max records to return (max 100) |
| domain_slug | string | null | Filter by domain slug |
| difficulty | string | null | Filter by difficulty (EASY, MEDIUM, HARD) |
| language | string | null | Filter by programming language |

#### Response (200 OK)
```json
[
  {
    "id": "660e8400-e29b-41d4-a716-446655440000",
    "title": "Next.js E-commerce",
    "slug": "vercel-nextjs-ecommerce",
    "description": "A full-featured e-commerce platform built with Next.js",
    "repoUrl": "https://github.com/vercel/nextjs-ecommerce",
    "repoOwner": "vercel",
    "repoName": "nextjs-ecommerce",
    "defaultBranch": "main",
    "downloadUrl": "",
    "liveUrl": "https://nextjs-ecommerce.demo",
    "downloadCount": 0,
    "domainId": "550e8400-e29b-41d4-a716-446655440000",
    "stars": 5200,
    "forks": 890,
    "language": "TypeScript",
    "techStack": ["Next.js", "React", "Tailwind CSS", "PostgreSQL"],
    "difficulty": "INTERMEDIATE",
    "topics": ["e-commerce", "react", "nextjs"],
    "projectType": "PROJECT",
    "author": "Vercel",
    "introduction": "A modern e-commerce solution...",
    "implementation": "Built with App Router...",
    "technicalSkills": ["React", "Next.js", "SQL"],
    "toolsUsed": ["Vercel", "PostgreSQL"],
    "conceptsUsed": ["SSR", "API Routes"],
    "subDomain": "Full-stack",
    "caseStudy": "Real-world e-commerce application...",
    "problemStatement": "Online shopping needs...",
    "solutionDescription": "This project provides...",
    "prerequisites": ["JavaScript basics", "React knowledge"],
    "prerequisitesText": "",
    "estimatedMinTime": 20,
    "estimatedMaxTime": 40,
    "deliverables": ["Product catalog", "Shopping cart"],
    "supposedDeadline": null,
    "optionalExtensions": null,
    "requirements": ["Node.js installed"],
    "requirementsText": "",
    "evaluationCriteria": null,
    "qaStatus": "APPROVED",
    "qaFeedback": null,
    "reviewedAt": "2026-02-01T00:00:00Z",
    "reviewedBy": "admin",
    "lastUpdated": "2026-03-01T12:00:00Z",
    "isActive": true,
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-03-01T12:00:00Z",
    "domain": {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Web Development",
      "slug": "web-development",
      "description": "Build modern web applications",
      "createdAt": "2026-01-01T00:00:00Z",
      "updatedAt": "2026-01-01T00:00:00Z"
    }
  }
]
```

---

### 4.2 Search Projects
**GET** `/api/projects/search`

Search projects by title or description.

#### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | Search query (min 2 characters) |

#### Response (200 OK)
```json
[
  {
    "id": "660e8400-e29b-41d4-a716-446655440000",
    "title": "Next.js E-commerce",
    ...
  }
]
```

---

### 4.3 Get Project by ID
**GET** `/api/projects/{project_id}`

Get full details for a single project.

#### Response (200 OK)
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "title": "Next.js E-commerce",
  ...
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 404 | Project not found | Project doesn't exist |

---

## 5. GitHub Projects

### 5.1 Get GitHub Projects (Paginated)
**GET** `/api/github-projects`

Get paginated list of GitHub projects with optional filtering.

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| page | integer | 1 | Page number |
| limit | integer | 24 | Items per page (max 100) |
| domainId | string | null | Filter by domain ID |
| difficulty | string | null | Filter by difficulty |
| search | string | null | Search term |
| sortBy | string | created_at | Sort field |
| order | string | desc | Sort order (asc/desc) |
| qaStatus | string | null | Filter by QA status |
| projectType | string | null | Filter by project type |

#### Response (200 OK)
```json
{
  "projects": [
    {
      "id": "660e8400-e29b-41d4-a716-446655440000",
      "title": "Next.js E-commerce",
      "slug": "vercel-nextjs-ecommerce",
      "description": "A full-featured e-commerce platform",
      "repoUrl": "https://github.com/vercel/nextjs-ecommerce",
      "repoOwner": "vercel",
      "repoName": "nextjs-ecommerce",
      "defaultBranch": "main",
      "downloadUrl": "",
      "downloadCount": 0,
      "domainId": "550e8400-e29b-41d4-a716-446655440000",
      "stars": 5200,
      "forks": 890,
      "language": "TypeScript",
      "techStack": [],
      "difficulty": "INTERMEDIATE",
      "topics": ["e-commerce", "react"],
      "projectType": "PROJECT",
      "author": "Vercel",
      "introduction": null,
      "implementation": null,
      "technicalSkills": [],
      "toolsUsed": [],
      "conceptsUsed": [],
      "subDomain": null,
      "caseStudy": null,
      "problemStatement": null,
      "solutionDescription": null,
      "prerequisites": [],
      "prerequisitesText": null,
      "estimatedMinTime": 10,
      "estimatedMaxTime": 40,
      "deliverables": [],
      "supposedDeadline": null,
      "optionalExtensions": null,
      "requirements": [],
      "requirementsText": null,
      "evaluationCriteria": null,
      "qaStatus": "PENDING",
      "qaFeedback": null,
      "reviewedAt": null,
      "reviewedBy": null,
      "lastUpdated": "2026-03-01T12:00:00Z",
      "isActive": true,
      "createdAt": "2026-01-01T00:00:00Z",
      "updatedAt": "2026-03-01T12:00:00Z",
      "domain": {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "Web Development",
        "slug": "web-development"
      }
    }
  ],
  "pagination": {
    "total": 100,
    "page": 1,
    "limit": 24,
    "totalPages": 5
  }
}
```

---

### 5.2 Get GitHub Project Languages
**GET** `/api/github-projects/languages`

Get all unique programming languages from cached GitHub projects.

#### Response (200 OK)
```json
[
  "TypeScript",
  "Python",
  "JavaScript",
  "Java",
  "Go",
  "Rust"
]
```

---

### 5.3 Search GitHub Projects
**GET** `/api/github-projects/search`

Search GitHub projects by title or description.

#### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | Search query |
| page | integer | 1 | Page number |
| limit | integer | 50 | Items per page |

#### Response (200 OK)
```json
{
  "projects": [...],
  "pagination": {
    "total": 50,
    "page": 1,
    "limit": 50,
    "totalPages": 1
  }
}
```

---

### 5.4 Get GitHub Projects by Domain
**GET** `/api/github-projects/domain/{domain_slug}`

Get GitHub projects filtered by domain slug.

#### Response (200 OK)
```json
{
  "projects": [...],
  "pagination": {
    "total": 200,
    "page": 1,
    "limit": 200,
    "totalPages": 1
  }
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 404 | Domain not found | Domain doesn't exist |

---

### 5.5 Get GitHub Project by ID
**GET** `/api/github-projects/{project_id}`

Get a single GitHub project by ID.
Automatically generates AI details if missing.

#### Response (200 OK)
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "title": "Next.js E-commerce",
  "slug": "vercel-nextjs-ecommerce",
  "description": "A full-featured e-commerce platform",
  "repoUrl": "https://github.com/vercel/nextjs-ecommerce",
  ...
  "caseStudy": "This e-commerce platform addresses the need for...",
  "problemStatement": "Modern retail requires...",
  "solutionDescription": "Built with Next.js 14 and PostgreSQL...",
  "prerequisites": ["JavaScript", "React basics", "SQL knowledge"],
  "deliverables": ["Product catalog", "User auth", "Checkout flow"],
  "estimatedMinTime": 20,
  "estimatedMaxTime": 40,
  ...
}
```

#### Errors
| Status | Code | Description |
|--------|------|-------------|
| 404 | GitHub project not found | Project doesn't exist |

---

### 5.6 Review GitHub Project
**POST** `/api/github-projects/{project_id}/review`

Submit a QA review for a GitHub project.

#### Request Payload
```json
{
  "qaStatus": "APPROVED",
  "qaFeedback": "Great project, suitable for learning",
  "reviewedBy": "admin"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| qaStatus | string | Yes | PENDING, APPROVED, REJECTED |
| qaFeedback | string | No | Feedback notes |
| reviewedBy | string | No | Reviewer identifier |

#### Response (200 OK)
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "title": "Next.js E-commerce",
  ...
  "qaStatus": "APPROVED",
  "qaFeedback": "Great project, suitable for learning",
  "reviewedAt": "2026-03-02T10:00:00Z",
  "reviewedBy": "admin"
}
```

---

## 6. Notifications

### 6.1 Get Notifications
**GET** `/api/notifications`

Get user notifications.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| page | integer | 1 | Page number |
| limit | integer | 30 | Items per page |

#### Response (200 OK)
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "message": "Welcome to Project Hub!",
    "type": "WELCOME",
    "relatedProjectId": null,
    "isRead": false,
    "createdAt": "2026-03-02T10:00:00Z"
  }
]
```

---

## 7. Analytics

### 7.1 Get Dashboard
**GET** `/api/analytics/dashboard`

Get analytics dashboard stats.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "projectsCompleted": 5,
  "totalTimeSpent": 120,
  "currentStreak": 5,
  "longestStreak": 10,
  "points": 1500,
  "achievementsCount": 3
}
```

---

### 7.2 Get Leaderboard
**GET** `/api/analytics/leaderboard`

Get user leaderboard.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | integer | 50 | Number of users to return |

#### Response (200 OK)
```json
[]
```

---

### 7.3 Update Streak
**POST** `/api/analytics/update-streak`

Update user's daily streak.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "message": "Streak updated",
  "currentStreak": 6,
  "longestStreak": 10
}
```

---

### 7.4 Get Time Tracking
**GET** `/api/analytics/time-tracking`

Get time tracking data.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| days | integer | 7 | Number of days |

#### Response (200 OK)
```json
[]
```

---

### 7.5 Get Progress Insights
**GET** `/api/analytics/progress-insights`

Get AI or algorithm-generated insights.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "insights": ["Keep up the good work!"]
}
```

---

## 8. Workspace

### 8.1 Get Active Timer
**GET** `/api/workspace/timer/active`

Get the currently active timer.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "session": null
}
```

---

## 9. Gamification

### 9.1 Get All Achievements
**GET** `/api/gamification/achievements/all`

Get all available achievements.

#### Response (200 OK)
```json
{
  "achievements": []
}
```

---

### 9.2 Get User Achievements
**GET** `/api/gamification/achievements/user`

Get achievements earned by the authenticated user.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "achievements": [],
  "progress": {
    "totalHours": 0,
    "completedProjects": 0,
    "currentStreak": 5,
    "points": 1500
  }
}
```

---

## 10. Learning Paths

### 10.1 Get Learning Paths
**GET** `/api/learning-paths`

Get all learning paths.

#### Response (200 OK)
```json
{
  "paths": []
}
```

---

### 10.2 Get Recommendations
**GET** `/api/learning-paths/recommendations`

Get personalized recommendations.

#### Headers
```
Authorization: Bearer <access_token>
```

#### Response (200 OK)
```json
{
  "recommendations": [],
  "insights": null
}
```

---

## Error Responses

All API endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "detail": "Invalid request parameters"
}
```

### 401 Unauthorized
```json
{
  "detail": "Could not validate credentials"
}
```

### 403 Forbidden
```json
{
  "detail": "Insufficient permissions"
}
```

### 404 Not Found
```json
{
  "detail": "Resource not found"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Internal server error"
}
```

---

## Rate Limiting

The AI project detail generation has a simple rate limiter:
- **10 requests per 60 seconds** per IP address
- Returns 429 when limit exceeded

---

## Notes

1. **AI Generation**: When accessing a GitHub project that lacks detailed information (case study, problem statement, etc.), the API will automatically generate these details using Cerebras AI (Llama 3.1) if available.

2. **GitHub Proxy**: The `/api/github-projects` endpoint proxies to the GitHub API for fresh data and caches results in the database.

3. **Pagination**: All list endpoints return pagination metadata in the response.

4. **Sorting**: GitHub projects support sorting by various fields (created_at, stars, forks, updated_at).

---

*Generated on: March 2026*
